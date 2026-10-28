const ElvOAction = require("../o-action").ElvOAction;
const fs = require("fs");
const xml2js = require("xml2js");

// Adapted from /Users/marc-olivier/Downloads/active-picture-convert.js (a standalone
// script that both converted an IMAX "active picture" XML file to JSON and set it on
// a mezzanine offering in one pass). Split into two independent actions here:
//   - CONVERT_ACTIVE_DATA: XML -> active_picture_data JSON (read-only against the fabric,
//     only needs the offering's video stream rate/time_base to compute frame-accurate ts values)
//   - SET_ACTIVE_DATA: uploads an already-converted active_picture_data JSON as a file on
//     the mezzanine and registers it as a new "active_picture" data stream on the offering
// Each independently re-derives whatever offering/video-stream info it needs, so either can
// be called on its own (e.g. SET_ACTIVE_DATA with a JSON produced by some other means).
//
// Uses xml2js (already a dependency here) rather than the original script's fast-xml-parser
// (not installed in this repo), and a small BigInt-based exact-fraction helper rather than
// the original's @eluvio/elv-js-helpers/Conversion/fraction (also not installed) - frame/time-base
// math needs exact rational arithmetic, not floating point, to reliably satisfy the
// "does this start frame land on an exact timestamp" check.

function bigGcd(a, b) {
    a = a < 0n ? -a : a;
    b = b < 0n ? -b : b;
    while (b) { [a, b] = [b, a % b]; }
    return a === 0n ? 1n : a;
}

class Fraction {
    constructor(a, b) {
        let n, d;
        if (b !== undefined) {
            n = BigInt(a); d = BigInt(b);
        } else if (a instanceof Fraction) {
            n = a.n; d = a.d;
        } else if (typeof a === "string" && a.includes("/")) {
            const [ns, ds] = a.split("/");
            n = BigInt(ns.trim()); d = BigInt(ds.trim());
        } else if (typeof a === "string" && a.includes(".")) {
            const [whole, frac] = a.split(".");
            n = BigInt((whole || "0") + frac);
            d = 10n ** BigInt(frac.length);
        } else {
            n = BigInt(Math.trunc(Number(a))); d = 1n;
        }
        if (d < 0n) { n = -n; d = -d; }
        const g = bigGcd(n, d);
        this.n = n / g; this.d = d / g;
    }
    static from(x) { return x instanceof Fraction ? x : new Fraction(x); }
    mul(other) { other = Fraction.from(other); return new Fraction(this.n * other.n, this.d * other.d); }
    div(other) { other = Fraction.from(other); return new Fraction(this.n * other.d, this.d * other.n); }
    inverse() { return new Fraction(this.d, this.n); }
    isInteger() { return this.d === 1n; }
    toFraction() { return this.n.toString() + "/" + this.d.toString(); }
    valueOf() { return Number(this.n) / Number(this.d); }
}

class ElvOActionImaxUtility extends ElvOAction {

    ActionId() {
        return "imax_utility";
    };

    Parameters() {
        return {
            parameters: {
                action: {type: "string", required: true, values: ["SET_ACTIVE_DATA", "CONVERT_ACTIVE_DATA"]}
            }
        };
    };

    IOs(parameters) {
        let inputs = {}, outputs = {};
        if (parameters.action === "CONVERT_ACTIVE_DATA") {
            inputs.xml_file_path = {type: "string", required: true};
            inputs.mezzanine_object_id = {type: "string", required: true};
            inputs.offering_key = {type: "string", required: false, default: "default"};
            inputs.config_url = {type: "string", required: false};
            inputs.private_key = {type: "password", required: false};
            outputs.active_picture_data = {type: "object"};
            outputs.time_base = {type: "string"};
            outputs.frame_rate = {type: "string"};
            outputs.duration = {type: "object"};
        }
        if (parameters.action === "SET_ACTIVE_DATA") {
            inputs.mezzanine_object_id = {type: "string", required: true};
            inputs.offering_key = {type: "string", required: false, default: "default"};
            inputs.active_picture_data = {type: "object", required: false};
            inputs.xml_file_path = {type: "string", required: false};
            inputs.write_token = {type: "string", required: false};
            inputs.config_url = {type: "string", required: false};
            inputs.private_key = {type: "password", required: false};
            outputs.filename = {type: "string"};
            outputs.write_token = {type: "string"};
            outputs.mezzanine_object_version_hash = {type: "string"};
        }
        return {inputs, outputs};
    };

    async Execute(inputs, outputs) {
        try {
            if (this.Payload.parameters.action === "CONVERT_ACTIVE_DATA") {
                return await this.executeConvertActiveData(inputs, outputs);
            }
            if (this.Payload.parameters.action === "SET_ACTIVE_DATA") {
                return await this.executeSetActiveData(inputs, outputs);
            }
        } catch (err) {
            this.Error("action_imax_utility failed", err);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };

    async getVideoStream(client, objectId, libraryId, offeringKey, writeToken) {
        const streams = await this.getMetadata({
            client, objectId, libraryId, writeToken,
            metadataSubtree: "offerings/" + offeringKey + "/media_struct/streams"
        });
        return Object.values(streams || {}).find(s => s && s.codec_type === "video");
    };

    // Shared by CONVERT_ACTIVE_DATA and SET_ACTIVE_DATA (when SET_ACTIVE_DATA is given
    // xml_file_path instead of an already-converted active_picture_data). Read-only
    // against the fabric - only needs the offering's video stream rate/time_base.
    async computeActivePictureData(client, objectId, libraryId, offeringKey, xmlFilePath) {
        this.reportProgress("Reading and parsing XML file", xmlFilePath);
        const xmlData = fs.readFileSync(xmlFilePath, "utf8");
        const parsed = await xml2js.parseStringPromise(xmlData, {explicitArray: false});
        let segments = parsed.activePictureMetadata.activePictureSegment;
        if (!Array.isArray(segments)) segments = [segments]; // xml2js won't array-wrap a lone element
        this.reportProgress(segments.length + " data entries found in XML file");

        const vidStream = await this.getVideoStream(client, objectId, libraryId, offeringKey);
        if (!vidStream) {
            return {vidStream: null};
        }

        const frameRateString = vidStream.rate;
        const timeBaseString = vidStream.time_base;
        const frameRate = new Fraction(frameRateString);
        const frameDur = frameRate.inverse();
        const timeBase = new Fraction(timeBaseString);

        this.reportProgress("Video stream frame rate: " + frameRateString);
        this.reportProgress("Video stream time base: " + timeBaseString);

        const activePictureData = {
            dash_id_scheme_uri: "urn:vubiquity:dash:active-picture:2026",
            data_type: "active_picture",
            timeline: {
                time_base: Number(timeBase.d),
                segments: []
            }
        };

        activePictureData.timeline.segments = segments.map((s) => {
            const startFrame = parseInt(s.startFrame, 10);
            const startTs = frameDur.mul(startFrame).div(timeBase);
            if (!startTs.isInteger()) {
                throw new Error(
                    "Start frame " + startFrame + " converts to non-integer TS " + startTs.toFraction() +
                    " with frame rate " + frameRate.toFraction() + " and time base " + timeBaseString
                );
            }

            const ap = s.activePicture;
            const rasterWidth = parseInt(ap.rasterWidth, 10);
            const rasterHeight = parseInt(ap.rasterHeight, 10);
            const matteTop = parseInt(ap.matteTop, 10);
            const matteBottom = parseInt(ap.matteBottom, 10);
            const matteLeft = parseInt(ap.matteLeft, 10);
            const matteRight = parseInt(ap.matteRight, 10);

            const top = new Fraction(matteTop).div(rasterHeight);
            const bottom = new Fraction(matteBottom).div(rasterHeight);
            const left = new Fraction(matteLeft).div(rasterWidth);
            const right = new Fraction(matteRight).div(rasterWidth);

            const arWidth = rasterWidth - matteLeft - matteRight;
            const arHeight = rasterHeight - matteTop - matteBottom;
            const aspectRatio = new Fraction(arWidth, arHeight);

            return {
                ts: Number(startTs.n),
                data: {
                    top: top.toFraction(),
                    bottom: bottom.toFraction(),
                    left: left.toFraction(),
                    right: right.toFraction(),
                    display_aspect_ratio: aspectRatio.toFraction()
                }
            };
        });

        return {activePictureData, vidStream, frameRateString, timeBaseString};
    };

    async executeConvertActiveData(inputs, outputs) {
        const client = await this.initializeActionClient();
        const objectId = inputs.mezzanine_object_id;
        const libraryId = await this.getLibraryId(objectId, client);
        const offeringKey = inputs.offering_key || "default";

        const {activePictureData, vidStream, frameRateString, timeBaseString} =
            await this.computeActivePictureData(client, objectId, libraryId, offeringKey, inputs.xml_file_path);
        if (!vidStream) {
            this.reportProgress("Video stream not found in offering", offeringKey);
            return ElvOAction.EXECUTION_FAILED;
        }

        outputs.active_picture_data = activePictureData;
        outputs.time_base = timeBaseString;
        outputs.frame_rate = frameRateString;
        outputs.duration = vidStream.duration;
        return ElvOAction.EXECUTION_COMPLETE;
    };

    async executeSetActiveData(inputs, outputs) {
        const client = await this.initializeActionClient();
        const objectId = inputs.mezzanine_object_id;
        const libraryId = await this.getLibraryId(objectId, client);
        const offeringKey = inputs.offering_key || "default";

        let activePictureData = inputs.active_picture_data;
        if (!activePictureData) {
            if (!inputs.xml_file_path) {
                this.reportProgress("Either active_picture_data or xml_file_path must be provided");
                return ElvOAction.EXECUTION_FAILED;
            }
            const converted = await this.computeActivePictureData(client, objectId, libraryId, offeringKey, inputs.xml_file_path);
            if (!converted.vidStream) {
                this.reportProgress("Video stream not found in offering", offeringKey);
                return ElvOAction.EXECUTION_FAILED;
            }
            activePictureData = converted.activePictureData;
        }

        const writeTokenSupplied = !!inputs.write_token;
        let writeToken = inputs.write_token;

        if (!writeToken) {
            writeToken = await this.getWriteToken({client, objectId, libraryId});
        }

        const vidStream = await this.getVideoStream(client, objectId, libraryId, offeringKey, writeToken);
        if (!vidStream) {
            this.reportProgress("Video stream not found in offering", offeringKey);
            return ElvOAction.EXECUTION_FAILED;
        }
        const frameRateString = vidStream.rate;
        const timeBaseString = vidStream.time_base;

        const filename = "active-picture-" + offeringKey + ".json";
        const jsonFileContents = JSON.stringify(activePictureData, null, 2);
        const buffer = Buffer.from(jsonFileContents, "utf8");

        this.reportProgress("Uploading JSON file to object", filename);
        await client.UploadFiles({
            libraryId, objectId, writeToken,
            fileInfo: [{
                path: filename,
                mime_type: "application/json",
                size: buffer.byteLength,
                data: buffer
            }]
        });

        const msStreamMetadata = {
            bit_rate: 100,
            codec_name: "none",
            codec_type: "data",
            dash_id_scheme_uri: "urn:vubiquity:dash:active-picture:2026",
            duration: vidStream.duration,
            label: "active_picture",
            optimum_seg_dur: {
                time_base: timeBaseString,
                ts: vidStream.duration.ts
            },
            rate: frameRateString,
            sources: [
                {
                    duration: vidStream.duration,
                    entry_point: {rat: "0", time_base: timeBaseString},
                    source: filename,
                    timeline_end: vidStream.duration,
                    timeline_start: {rat: "0", time_base: timeBaseString}
                }
            ],
            start_time: {time_base: timeBaseString, ts: 0},
            time_base: timeBaseString
        };

        const poStreamMetadata = {
            encryption_schemes: {},
            representations: {
                "active_picture-data": {
                    bit_rate: 100,
                    media_struct_stream_key: "active_picture",
                    type: "RepData"
                }
            }
        };

        this.reportProgress("Writing stream metadata for offering", offeringKey);
        await client.ReplaceMetadata({
            libraryId, objectId, writeToken, metadata: msStreamMetadata,
            metadataSubtree: "offerings/" + offeringKey + "/media_struct/streams/active_picture"
        });
        await client.ReplaceMetadata({
            libraryId, objectId, writeToken, metadata: poStreamMetadata,
            metadataSubtree: "offerings/" + offeringKey + "/playout/streams/active_picture"
        });

        outputs.filename = filename;
        outputs.write_token = writeToken;

        if (writeTokenSupplied) {
            this.reportProgress("Write token left open (was supplied by caller)", writeToken);
            return ElvOAction.EXECUTION_COMPLETE;
        }

        const result = await this.FinalizeContentObject({
            objectId, libraryId, writeToken, client,
            commitMessage: "Add active-picture.json data and stream to offering " + offeringKey
        });
        if (result && result.hash) {
            outputs.mezzanine_object_version_hash = result.hash;
            return ElvOAction.EXECUTION_COMPLETE;
        }
        this.reportProgress("Could not finalize", result);
        return ElvOAction.EXECUTION_EXCEPTION;
    };

    static VERSION_HISTORY = {
        "0.0.1": "2026-09-29 - Initial version, adapted from active-picture-convert.js",
        "0.0.2": "2026-09-29 - SET_ACTIVE_DATA: active_picture_data is now optional; accepts xml_file_path instead, converting internally before uploading/setting in one step"
    };
    static VERSION = "0.0.2";
}

if (ElvOAction.executeCommandLine(ElvOActionImaxUtility)) {
    ElvOAction.Run(ElvOActionImaxUtility);
} else {
    module.exports = ElvOActionImaxUtility;
}
