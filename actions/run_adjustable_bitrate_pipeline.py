#!/usr/bin/env python3

##############################################################################
# This program is protected under international and U.S. copyright laws as
# an unpublished work. This program is confidential and proprietary to the
# copyright owners. Reproduction or disclosure, in whole or in part, or the
# production of derivative works therefrom without the express permission of
# the copyright owners is prohibited.
#
#                Copyright (C) 2020-2024 by Dolby International AB.
#                            All rights reserved.
###############################################################################

"""
    End-to-end encoding.

    For usage, see:
        python3 run_pipeline.py --help

    Example command lines for different encoding- and transcoding usecases:
    - Mezzanine to Profile 5 encoding
        run_pipeline.py --input <input.mxf> --output <output_folder> --profile 5

    - HDR10 to Profile 5
        run_pipeline.py --input <input.mxf> --frame-rate <fps> --start-frame <startframe> --end-frame <endframe> --output <output_folder> --profile <profile> --resolution <Width>x<Height> --generate-metadata

    Prerequisites:
        - Linux / Windows / Mac
        - Python 3
"""

import argparse
import atexit
import math
import os
from pathlib import Path
import platform
import re
import subprocess
import sys
import tempfile
import time

# ---------------------------------------------------------------------------
# Path resolution — use SIDK_DIR env var so this script can live outside the
# SIDK tree while still finding impact_encode.py and the encoder binaries.
# ---------------------------------------------------------------------------
_sidk_dir_env = os.environ.get('SIDK_DIR')
HERE = Path(_sidk_dir_env) / 'Test_Tools' / 'scripts' if _sidk_dir_env else Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# Global state (mirrors common.py module-level variables)
# ---------------------------------------------------------------------------
resolution = None
start_frame = None
end_frame = None
frame_rate = None
global_string = None
sei_file = None


class Error(Exception):
    '''Expected, user-facing errors'''
    pass


# ---------------------------------------------------------------------------
# Temporary raw-image management
# ---------------------------------------------------------------------------
class TempRawImages:
    def __init__(self):
        self._paths = list()
        self._files = list()

    def add(self, filename, output_dir, num_frames):
        if num_frames <= 1000:
            tempdir = tempfile.TemporaryDirectory()
            self._paths.append(tempdir)
            return os.path.join(tempdir.name, filename)
        else:
            path = os.path.join(output_dir, filename)
            self._files.append(path)
            return path

    def delete(self):
        for tempdir in self._paths:
            try:
                tempdir.cleanup()
            except Exception as e:
                print("Could not remove dir {}: {}".format(tempdir.name, e))
        for file_ in self._files:
            try:
                os.remove(file_)
            except Exception as e:
                print("Could not remove file {}: {}".format(file_, e))


raw_images = TempRawImages()
atexit.register(lambda: raw_images.delete())


# ---------------------------------------------------------------------------
# Runtime reporting
# ---------------------------------------------------------------------------
class ReportRuntimes:
    def __init__(self):
        self._runtimes = list()

    def add(self, cmd, time_):
        self._runtimes.append((cmd, time_))

    def report(self):
        print("Runtime breakdown (total {:.2f} s)".format(sum(time_ for _, time_ in self._runtimes)))
        for cmd, time_ in self._runtimes:
            print("    {:20} {:.2f} s".format(cmd[0] + ":", time_))


class ProfileReport:
    def __init__(self):
        self._files = list()

    def append(self, file):
        self._files.append(file)

    def num_files(self):
        return len(self._files)

    def write_profile(self):
        if self.num_files() == 0:
            return
        dynamic_report = HERE / "dynamic_report.py"
        cmd = [sys.executable, str(dynamic_report)]
        cmd += ["--title", os.environ.get("PROFILER_TITLE", "")]
        cmd += self._files
        print(" ".join(cmd))
        result = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, encoding="utf-8")
        report = Path("profile_report.txt")
        report.write_text(result.stdout)
        print("Wrote", report)
        print(result.stdout)


runtimes = ReportRuntimes()
profile_report = ProfileReport()
atexit.register(lambda: runtimes.report())
atexit.register(lambda: profile_report.write_profile())


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def _cpu_count():
    if platform.system() == "Linux":
        return len(os.sched_getaffinity(0))
    else:
        return os.cpu_count()


def _get_output(command, fatal_error=True):
    global runtimes
    time_0 = time.time()
    try:
        return subprocess.check_output(command).decode()
    except FileNotFoundError as error:
        if not fatal_error:
            return ""
        print(error, file=sys.stderr)
        print("Please ensure that '{}' exists".format(command[0]), file=sys.stderr)
        sys.exit(1)
    time_1 = time.time()
    runtimes.add(command, time_1 - time_0)


def run(args, command):
    global runtimes
    global profile_report
    time_0 = time.time()
    print(" ".join(command))
    if not args.dry_run:
        if args.performance_dir is not None and not ("dolbyhevcenc" in Path(command[0]).name):
            profile_txt = f"profile_{profile_report.num_files()}.txt"
            os.environ["PROFILER_DYNAMIC"] = "1"
            os.environ["PROFILEROUT"] = profile_txt
            profile_report.append(profile_txt)
        try:
            subprocess.check_call(command)
        except FileNotFoundError as error:
            print(error, file=sys.stderr)
            print("Please ensure that '{}' exists".format(command[0]), file=sys.stderr)
            sys.exit(1)
    time_1 = time.time()
    runtimes.add(command, time_1 - time_0)


def get_path(args, application):
    if application == "metafier":
        subdir = {
            "Linux": "linux/" + platform.machine(),
            "Darwin": "mac",
            "Windows": "win",
        }[platform.system()]
        return HERE.parent.parent / "Test_Tools" / "metafier" / subdir / "metafier"
    subdir = {
        "Linux": "linux/" + platform.machine(),
        "Darwin": "mac",
        "Windows": "win",
    }[platform.system()]
    binaries_dir = HERE.parent.parent / "Code" / "bin" / subdir / "static"
    if application != "dolbyhevcenc":
        if args.performance_dir is not None:
            binaries_dir = Path(args.performance_dir)
        elif os.environ.get("DVES_BINARY_PATH"):
            binaries_dir = Path(os.environ["DVES_BINARY_PATH"])
    return binaries_dir / application


def output_filename(args, tool, extension, name, suffix=""):
    if name is None:
        filename = "{}_{}x{}@{}fps_{}_{}{}.{}".format(
            profile_string,
            resolution[0], resolution[1],
            frame_rate,
            start_frame, end_frame,
            suffix, extension
        )
    else:
        filename = "{}_{}_{}x{}@{}fps_{}_{}{}.{}".format(
            name, profile_string,
            resolution[0], resolution[1],
            frame_rate,
            start_frame, end_frame,
            suffix, extension
        )

    output_dir = os.path.join(args.output, tool)
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    global raw_images
    if (filename.endswith("yuv") or filename.endswith("rgb")) and not args.keep_raw_data:
        return raw_images.add(filename, output_dir, end_frame - start_frame + 1)
    else:
        return os.path.join(output_dir, filename)


def timescale_duration():
    if frame_rate is None:
        raise Error("Missing --frame-rate")
    if "." in frame_rate:
        timescale_duration_map = {
            "23.98":  ( 24000, 1001),
            "23.976": ( 24000, 1001),
            "47.95":  ( 48000, 1001),
            "95.9":   ( 96000, 1001),
            "95.90":  ( 96000, 1001),
            "29.97":  ( 30000, 1001),
            "59.94":  ( 60000, 1001),
            "119.88": (120000, 1001),
        }
        if frame_rate not in timescale_duration_map:
            raise Error("Unrecognized frame rate '{}', expected integer or one of {}".format(
                frame_rate, ", ".join(timescale_duration_map)))
        timescale, duration = timescale_duration_map[frame_rate]
    else:
        timescale, duration = int(frame_rate), 1
    return timescale, duration


def _vui(output_format, profile):
    parameter_names = (
        "--video-format",
        "--video-full-range",
        "--color-primaries",
        "--transfer-characteristics",
        "--matrix-coefficients",
    )
    if output_format == "SDR":
        values = (5, 0, 1, 1, 1)
    else:
        values = {
            "5":    (5, 1, 2,  2,  2),
            "8.1":  (5, 0, 9, 16,  9),
            "20.0": (0, 1, 9, 16, 15),
        }["8.1" if output_format == "HDR10" else profile]

    return [arg
            for pair in zip(parameter_names, map(str, values))
            for arg in pair]


# ---------------------------------------------------------------------------
# Encoders
# ---------------------------------------------------------------------------
def hevc_encoder(yuv, args):
    output_BL_filename = output_filename(args, "BLencoder", "265", name="BL")
    if args.video_encoder is None:
        hevc_encode_script = str(HERE / "impact_encode.py")
    else:
        hevc_encode_script = args.video_encoder
    timescale, duration = timescale_duration()

    width, height = resolution
    if hasattr(args, "aspect_ratio_type") and args.aspect_ratio_type == 3:
        pad = args.pad.split("x")
        width += int(pad[0]) + int(pad[1])
        height += int(pad[2]) + int(pad[3])

    vui_params = _vui(args.output_format, args.profile)

    # Use VBR when bitrate is specified, constant quality otherwise.
    use_bitrate = args.bitrate is not None
    run_control_mode = "vbr-first" if use_bitrate else "cq"

    hevc_args = [
        "--input", yuv,
        "--resolution", f"{width}x{height}",
        "--output", output_BL_filename,
        "--frame-rate", f"{timescale},{duration}",
        "--pixel-aspect-ratio", "1,1",
        "--rc-mode", run_control_mode,
        "--max-hevc-level", "6.1" if args.profile == "20.0" else "6.2",
        "--tier", "high" if args.profile == "20.0" else "main",
        "--profile", "multiview10" if args.profile == "20.0" else "main10",
        "--num-cores", str(args.num_cores if args.num_cores is not None else _cpu_count()),
    ] + (
        ["--dry-run"] if args.dry_run else []
    ) + (
        ["--sei-file", sei_file] if sei_file is not None else []
    ) + (
        ["--bitrate", str(args.bitrate)] if use_bitrate else
        (["--quality", str(args.quality)] if args.quality is not None else [])
    ) + (
        ["--reference-displays-info-sei-3d", "31,false,0,0,1,0,0,0,0,false,0"] if args.profile == "20.0" else []
    ) + (
        ["--chroma-sample-location", "2,2"] if args.profile == "20.0" else []
    ) + vui_params

    hevc_encoder_binary = get_path(args, "dolbyhevcenc")
    env = dict(os.environ)
    env["PATH"] = str(hevc_encoder_binary.parent) + os.pathsep + env["PATH"]
    cmd = [sys.executable, hevc_encode_script] + hevc_args
    print(" ".join(cmd))
    subprocess.run(cmd, check=True, env=env)

    return output_BL_filename


def av1_encoder(bl_yuv, args):
    assert args.output_format == "DolbyVision" and args.profile in ("10.0", "10.1"), \
        f"Unsupported encoding to AV1 {args.output_format} {args.profile}"

    output_BL_filename = output_filename(args, "BLencoder", "obu", name="BL")
    if args.video_encoder is None:
        raise Error("Please provide --video-encoder script for AV1 encoding")
    else:
        av1_encode_script = args.video_encoder
    timescale, duration = timescale_duration()

    width, height = resolution

    av1_args = [
        "--input", str(bl_yuv),
        "--resolution", f"{width}x{height}",
        "--output", output_BL_filename,
        "--frame-rate", f"{timescale},{duration}",
        "--num-cores", str(args.num_cores if args.num_cores is not None else _cpu_count()),
        "--video-full-range", {"10.0": "1", "10.1": "0"}[args.profile],
    ] + (
        ["--color-primaries", "bt2020"] if args.profile == "10.1" else []
    ) + (
        ["--transfer-characteristics", "smpte2084"] if args.profile == "10.1" else []
    ) + (
        ["--matrix-coefficients", "bt2020ncl"] if args.profile == "10.1" else []
    ) + (
        ["--dry-run"] if args.dry_run else []
    ) + (
        ["--sei-file", sei_file] if sei_file is not None else []
    ) + (
        ["--bitrate", str(args.bitrate)] if args.bitrate is not None else []
    )

    cmd = [sys.executable, av1_encode_script] + av1_args
    print(" ".join(cmd))
    subprocess.run(cmd, check=True)

    return output_BL_filename


# ---------------------------------------------------------------------------
# TIFF / JPEG / MXF probing helpers
# ---------------------------------------------------------------------------
def _parse_tiff_jpeg_size(path):
    stdout = _get_output(["file", path], fatal_error=False).strip()
    match = re.search("width=([^,]*)", stdout)
    if match is not None:
        width = match.group(1)
        height = re.search("height=([^,]*)", stdout).group(1)
        return int(width), int(height)

    stdout = _get_output(["magick", "identify", path], fatal_error=False)
    match = re.search(" ([0-9]*)x([0-9]*) ", stdout)
    if match is not None:
        return int(match.group(1)), int(match.group(2))
    else:
        raise Error("{}: Could not get TIFF / J2K image size".format(path))


def _num_frames(raw_file_path, input_format, resolution):
    width, height = resolution
    file_size = os.path.getsize(raw_file_path)
    if 'u8' in input_format:
        bytes_per_pixel = 1
    else:
        bytes_per_pixel = 2
    if '444' in input_format:
        frame_size = width * height * bytes_per_pixel * 3
    elif '420' in input_format:
        frame_size = width * height * bytes_per_pixel * 3 // 2
    else:
        raise Error(f"Invalid format string {input_format}, must contain 444 or 420.")
    if not file_size % frame_size == 0:
        raise Error(f"{raw_file_path} size ({file_size} bytes) is not a multiple of the frame size {frame_size}")
    return file_size // frame_size


def _analyze_mxf(args, path, segmented):
    if "," in path:
        path = path.split(",")[0]
    if not os.path.exists(path):
        raise Error(f"File '{path}' not found")
    generate_metadata = (hasattr(args, "generate_metadata") and args.generate_metadata)
    if args.metadata is None:
        metafier = get_path(args, "metafier")
        try:
            stdout = _get_output([str(metafier), "--show-info", "-", path])
        except subprocess.CalledProcessError:
            if not generate_metadata:
                raise Error(f"{path}: Missing metadata. Consider setting --metadata <md.xml> or --generate-metadata")
            else:
                return None, None, None, []
        frames, width, height, mxf_frame_rate = None, None, None, None
        for line in stdout.splitlines():
            if line.startswith("Size:"):
                width, height = map(int, line.split(" ")[1:3])
            elif line.startswith("Frames:"):
                frames = int(line.split(" ")[1])
            elif line.startswith("FPS:"):
                s = line.split(" ")[1]
                if '.' not in s:
                    mxf_frame_rate = s
        if width is None or height is None or frames is None:
            raise Error("{}: Could not get image size, number of frames and frame rate".format(path))
        return (width, height), frames, mxf_frame_rate, list()
    else:
        if generate_metadata:
            return None, None, None, []
        else:
            return None, None, None, ["-inputMetadata", args.metadata]


def _preproc_cli_options(args, resize=None, begin_switch="-startFrame", segmented=False):
    global resolution, start_frame, end_frame, frame_rate
    frame_rate = args.frame_rate
    if any(args.input.endswith(ext) for ext in (".tiff", ".j2k", ".jpg", ".jpeg")):
        if args.start_frame is None:
            raise Error("Please provide start frame for TIFF/J2K input")
        if args.end_frame is None:
            raise Error("Please provide end frame for TIFF/J2K input")
        start_frame = args.start_frame
        end_frame = args.end_frame
        if args.resolution is not None:
            resolution = tuple(map(int, args.resolution.split("x")))
        else:
            path_, filename_ = os.path.split(args.input)
            if '#' in filename_:
                filename_ = filename_.replace('#', '0')
            else:
                filename_ = filename_ % start_frame
            resolution = _parse_tiff_jpeg_size(os.path.join(path_, filename_))
        inputs = ["-inputMezzanine", args.input]
        if args.metadata is None and not args.generate_metadata:
            raise Error("Set --generate-metadata or use --metadata for TIFF/J2K input")

    elif any(args.input.endswith(ext) for ext in (".rgb", ".yuv")):
        if args.input_format is None:
            raise Error("Option --input-format is required for RGB/YUV input.")
        if args.resolution is not None:
            resolution = tuple(map(int, args.resolution.split("x")))
        else:
            raise Error("Missing --resolution")
        start_frame = args.start_frame if args.start_frame is not None else 0
        end_frame = args.end_frame if args.end_frame is not None else \
            _num_frames(args.input, args.input_format, resolution) - 1
        inputs = ["-inputRaw", args.input]
        if args.metadata is None and not args.generate_metadata:
            raise Error("Set --generate-metadata or use --metadata for RGB/YUV input")

    elif args.input.endswith(".mxf") or \
            (args.input.lower().endswith(".mov") and "," in args.input):
        resolution, frames, detected_frame_rate, inputs = _analyze_mxf(args, args.input, segmented)
        if resolution is None:
            if args.resolution is None:
                raise Error("Missing --resolution")
            if args.start_frame is None:
                raise Error("Missing --start-frame")
            if args.end_frame is None:
                raise Error("Missing --end-frame")
        if args.resolution is not None:
            resolution = tuple(map(int, args.resolution.split("x")))
        if args.frame_rate is not None:
            frame_rate = args.frame_rate
        start_frame = args.start_frame if args.start_frame is not None else 0
        if args.end_frame is None:
            if frames is None:
                raise Error("Missing --end-frame")
            end_frame = frames - 1
        else:
            end_frame = args.end_frame
        inputs += ["-inputMezzanine", args.input]

    elif args.input.lower().endswith(".mov"):
        if args.resolution is None:
            raise Error("Missing --resolution")
        resolution = tuple(map(int, args.resolution.split("x")))
        start_frame = args.start_frame if args.start_frame is not None else 0
        if args.end_frame is None:
            raise Error("Missing --end-frame")
        end_frame = args.end_frame
        if frame_rate is None:
            raise Error("Missing --frame-rate")
        inputs = ["-inputMezzanine", args.input]
        inputs += ["-inputFormat", str(frame_rate) + 'fps']
        if args.metadata is None and not args.generate_metadata:
            raise Error("Set --generate-metadata or use --metadata for ProRes input")
    else:
        raise Error("{}: Sorry, source format is not supported".format(args.input))

    if resize is not None:
        resolution = tuple(map(int, resize.split("x")))
        inputs += ["-targetSize", "{}x{}".format(*resolution)]

    inputs += [begin_switch, str(start_frame), "-endFrame", str(end_frame)]
    if args.metadata is not None:
        inputs += ["-inputMetadata", args.metadata]

    return inputs


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------
def dv_preproc(args, segmented=False, xml_out=False):
    global sei_file, resolution

    PREPROC = get_path(args, "DVPreproc")

    inputs = _preproc_cli_options(args, args.resize, begin_switch="-beginFrame", segmented=segmented)
    if segmented:
        inputs += ["-segmented"]
    if hasattr(args, "input_format") and args.input_format is not None:
        inputs += ["-inputFormat", args.input_format]
    if args.metadata_offset is not None:
        inputs += ["-metadataOffset", args.metadata_offset]
    if args.generate_metadata:
        inputs += ["-generateMetadata"]
        if args.mastering_display is not None:
            inputs += ["-masteringDisplay", args.mastering_display]
        if args.level11 is not None:
            inputs += ["-setL11", args.level11]
        if args.letterbox is not None:
            inputs += ["-letterbox", args.letterbox]
    if args.level11 is not None:
        inputs += ["-updateL11", args.level11]
    if args.num_cores is not None:
        inputs += ["-numThreads", args.num_cores]

    if xml_out:
        xml_file = output_filename(args, "MetadataGenerator", "xml", name="metadata")
        if args.output_format == "DolbyVision":
            inputs += ["-profile", args.profile, "-skipPreproc"]
        if segmented:
            inputs += ["-disableTemporalFiltering"]
            l1_filtering_done = False
        else:
            l1_filtering_done = True
        outputs = ["-outputMetadata", xml_file]
    else:
        if args.generate_metadata:
            inputs += ["-disableTemporalFiltering"]
            l1_filtering_done = False
        else:
            l1_filtering_done = True

        inputs += ["-outputFormat", args.output_format]
        if args.output_format == "DolbyVision":
            inputs += ["-profile", args.profile]

        rpu = output_filename(args, "PREPROC", "bin", name="RPU")
        outputs = ["-outputRPU", rpu]
        if args.output_format == "DolbyVision" and args.profile == '20.0':
            base_layer_left = output_filename(args, "PREPROC", "yuv", name="BLLeft")
            base_layer_right = output_filename(args, "PREPROC", "yuv", name="BLRight")
            base_layer = f"{base_layer_left},{base_layer_right}"
        else:
            if args.input is not None and ',' in args.input:
                resolution = 2 * resolution[0], resolution[1]
            base_layer = output_filename(args, "PREPROC", "yuv", name="BL")
        outputs += ["-outputBLYUV", base_layer]

        if args.output_format == "HDR10" or \
                (args.output_format == "DolbyVision" and args.profile in ('8.1', '10.1')):
            sei_file = output_filename(args, "PREPROC", "txt", name="SEI")
            outputs += ["-outputHDR10SEI", sei_file]
        if (args.output_format == "DolbyVision" and args.profile in ("8.1", "10.1")) or \
                args.output_format == "HDR10":
            outputs += ["-contentMapping", args.content_mapping]
        if args.output_format == "DolbyVision" and args.profile in ('5', '10.0', '20.0'):
            if hasattr(args, "max_scene_frames") and args.max_scene_frames is not None:
                inputs += ["-maxSceneFrames", args.max_scene_frames]

        if args.debug:
            if args.output_format == "DolbyVision" and args.profile not in ("5", "10.0"):
                hdr_yuv = output_filename(args, "PREPROC", "yuv", name="HDR")
                outputs += ["-outputHDRYUV", hdr_yuv]
            hdr_rgb = output_filename(args, "PREPROC", "rgb", name="HDR")
            outputs += ["-outputHDRRGB", hdr_rgb]

    if hasattr(args, "aspect_ratio_type"):
        if args.aspect_ratio_type == 3:
            inputs += ["-aspectRatioMode", str(args.aspect_ratio_type)]
            if args.crop:
                inputs += ["-crop", args.crop]
            if args.pad:
                inputs += ["-pad", args.pad]
        elif args.aspect_ratio_type:
            inputs += ["-aspectRatioMode", str(args.aspect_ratio_type)]

    run(args, [str(PREPROC)] + inputs + outputs)

    if xml_out:
        return xml_file, l1_filtering_done
    else:
        return base_layer, rpu, l1_filtering_done


def ves_muxer(bitstream, rpu, args, bitstream_standard):
    VES_MUXER = get_path(args, "ves_muxer")
    muxed = output_filename(
        args, "Vesmuxer",
        {'HEVC': '265', 'AV1': 'obu'}[bitstream_standard],
        name="BL_RPU")

    run(args,
        [str(VES_MUXER),
         "-s", bitstream_standard,
         "-b", bitstream,
         "-r", rpu,
         "-o", muxed,
         ])
    return muxed


def metadata_postproc(muxed, args, codec, rpu_compression=None, L1_filter=False):
    assert codec in ('HEVC', 'AV1')
    METADATA_POSTPROC = get_path(args, "metadata_postproc")

    dves_bitstream = output_filename(
        args, "MetadataPostProcessor",
        {"HEVC": "265", "AV1": "obu"}[codec],
        name="BL_RPU",
        suffix="_mdpp")

    if args.output_format == "DolbyVision":
        inputs = ["-inputVesType", "1", "-inputMuxedVes", muxed]
        outputs = ["-outputMuxedVes", dves_bitstream]
    elif args.output_format == "HDR10":
        inputs = ["-inputVesType", "3", "-inputBLVes", muxed]
        outputs = ["-outputBLVes", dves_bitstream]
    else:
        assert False, f"Unknown bitstream format {args.output_format}"

    parameters = []
    if rpu_compression is not None:
        parameters += ["-RPUcompressType", {False: "0", True: "1"}[rpu_compression]]

    if L1_filter:
        inputs += ["-filterL1", "1"]
        if args.profile != "20.0":
            inputs += ["-generateL2"]
        inputs += ["-frameRate", frame_rate]

    if args.profile in ("10.0", "10.1"):
        inputs += ["-numFrames", str(end_frame - start_frame + 1)]

    run(args,
        [str(METADATA_POSTPROC), "-inputVideoStd", codec]
        + inputs + parameters + outputs)

    return dves_bitstream


# ---------------------------------------------------------------------------
# Initialisation and argument parsing
# ---------------------------------------------------------------------------
def init(args):
    global profile_string

    if args.output_format != 'DolbyVision':
        if args.profile is not None:
            raise Error(f"--profile cannot be used with --output-format {args.output_format}")
    if hasattr(args, "mastering_display") and args.mastering_display is None:
        args.mastering_display = "44" if args.profile == "20.0" else None
    if args.input is None:
        raise Error("Missing --input")
    if args.output_format == "SDR":
        profile_string = "sdr"
    elif args.output_format == "HDR10":
        profile_string = "hdr10"
        if args.content_mapping == "none":
            profile_string += "-nomap"
        elif args.content_mapping == "dynamic":
            profile_string += "-mapDynamic1000"
        else:
            raise Error("Missing --content-mapping for HDR10 output")
    else:
        if args.profile is None:
            raise Error("Missing --profile")
        elif args.profile == "5":
            profile_string = "dvhe-05"
        elif args.profile == "8.1":
            profile_string = "dvhe-08"
            if args.content_mapping == "none":
                profile_string += "-nomap-81"
            elif args.content_mapping == "dynamic":
                profile_string += "-mapDynamic1000-81"
            else:
                raise Error("Missing --content-mapping for profile 8.1")
        elif args.profile == "10.0":
            profile_string = "dav1-10-100"
        elif args.profile == "10.1":
            profile_string = "dav1-10"
            if args.content_mapping == "none":
                profile_string += "-nomap-101"
            elif args.content_mapping == "dynamic":
                profile_string += "-mapDynamic1000-101"
            else:
                raise Error("Missing --content-mapping for profile 10.1")
        elif args.profile == "20.0":
            profile_string = "dvhe-20"
        else:
            assert False, args.profile


def add_arguments(parser, segmented=False):
    parser.add_argument('--video-encoder', metavar="PATH",
                        help="HEVC/AV1 encoder script. Required for AV1-based profiles.")
    parser.add_argument('-b', '--bitrate', metavar="NUM", type=int, default=None,
                        help="HEVC/AV1 encoding average bitrate (bits per second). "
                             "Default: quality-based CQ mode for HEVC")
    if segmented:
        parser.add_argument('-s', '--start-frame', metavar="NUM,NUM", required=True,
                            help="First input frame for each segment")
        parser.add_argument('-e', '--end-frame', metavar="NUM,NUM", required=True,
                            help="Last input frame for each segment")
        parser.add_argument('--metadata-offset', metavar="NUM",
                            help="Frame offset to add when reading frames from the metadata input. Default: 0")
    parser.add_argument('-n', '--dry-run',
                        help="Display, do not run commands", action="store_true")
    parser.add_argument('-c', '--num-cores',
                        help="Number of threads used. Default: " + str(_cpu_count()))
    parser.add_argument('--keep-raw-data',
                        help="Do not delete intermediate .yuv and .rgb files",
                        default=False, action="store_true")
    return parser


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    args = _create_parser().parse_args(sys.argv[1:])
    init(args)
    _run_pipeline(args)


def _create_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', metavar="PATH",
                        help="Input file (MXF, ProRes, TIFF/J2K, RGB/YUV, or comma-separated dual files)")
    parser.add_argument('--input-format', metavar="STR",
                        help='Input format string for HDR input. Example: "pq p3d65 rgb computer 24fps"')
    parser.add_argument('-o', '--output', metavar="PATH",
                        required=True, help="Output directory")
    parser.add_argument('-r', '--frame-rate', metavar="NUM",
                        help="Encoding frame rate (frames per second)")
    parser.add_argument('-m', '--metadata',
                        help="Display Management Metadata XML")
    parser.add_argument('-f', '--output-format', choices=("DolbyVision", "HDR10", "SDR"),
                        default="DolbyVision", help="Default: DolbyVision")
    parser.add_argument('-p', '--profile', choices=("5", "8.1", "10.0", "10.1", "20.0"))
    parser.add_argument('--content-mapping', choices=("none", "dynamic"),
                        help="Content mapping for profile 8.1/10.1 and for HDR10")
    parser.add_argument('-s', '--start-frame', metavar="NUM",
                        type=int, help="Default: first input frame")
    parser.add_argument('-e', '--end-frame', metavar="NUM",
                        type=int, help="Default: last input frame")
    parser.add_argument('--metadata-offset', metavar="NUM",
                        help="Frame offset for metadata input. Default: 0")
    parser.add_argument('--resolution', metavar="<WIDTH>x<HEIGHT>",
                        help="Default: autodetect")
    parser.add_argument('--resize', metavar="<WIDTH>x<HEIGHT>",
                        help="Default: no resize. Example --resize 1920x1080")
    parser.add_argument('--aspect-ratio-type', metavar="NUM", type=int, default=2,
                        choices=(0, 1, 2, 3),
                        help="0=crop, 1=pad, 2=stretch, 3=manual. Default: 2")
    parser.add_argument('--crop', metavar="<LEFTxRIGHTxTOPxDOWN>",
                        help="Pixels to crop. Example --crop 0x0x220x220")
    parser.add_argument('--pad', metavar="<LEFTxRIGHTxTOPxDOWN>",
                        help="Pixels to pad. Example --pad 110x110x400x400")
    parser.add_argument('--letterbox', metavar="<LEFTxRIGHTxTOPxDOWN>",
                        help="Letterbox for L5 generation. Example --letterbox 140x140x0x0")
    parser.add_argument('-q', '--quality', metavar="NUM", type=int, default=600,
                        help="Quality level 0–1000 (CQ mode only). Default: 600")
    parser.add_argument('--max-scene-frames', metavar="NUM",
                        help="Max frames per scene for profiles 5, 10.0 and 20.0")
    parser.add_argument('--level11', metavar="content_type,white_point",
                        help="Add/overwrite L11 metadata. Example: --level11 1,0")
    parser.add_argument('--generate-metadata', action="store_true", default=False,
                        help="Generate metadata for non-DV bitstreams (default: disabled)")
    parser.add_argument('--mastering-display', default=None,
                        help="Mastering display ID for metadata generation")
    parser.add_argument('--dual-pass', action="store_true", default=False,
                        help="Two-pass metadata generation (default: disabled)")
    parser.add_argument('--performance-dir', metavar="DIR", type=str,
                        help="Directory with performance-instrumented binaries")
    parser.add_argument('-d', '--debug',
                        help="Reconstructed HDR output for debug", action="store_true")
    return add_arguments(parser)


def _run_pipeline(args, segmented=False):
    if args.generate_metadata and args.dual_pass:
        print("Dual-pass metadata generation")
        xml_file, l1_filtering_done = dv_preproc(args, segmented, xml_out=True)
        assert l1_filtering_done
        args.generate_metadata = False
        args.metadata = xml_file
        args.mastering_display = None
        base_layer, rpu, _ = dv_preproc(args, segmented)
    else:
        if args.generate_metadata:
            print("Single-pass metadata generation")
        base_layer, rpu, l1_filtering_done = dv_preproc(args, segmented)

    if args.output_format in ("SDR", "HDR10") or args.profile in ("5", "8.1", "20.0"):
        codec = "HEVC"
        video = hevc_encoder(base_layer, args)
    else:
        assert args.output_format == "DolbyVision"
        assert args.profile in ("10.0", "10.1")
        codec = "AV1"
        video = av1_encoder(base_layer, args)

    if args.output_format in ("SDR", "HDR10"):
        muxed = video
    else:
        muxed = ves_muxer(video, rpu, args, bitstream_standard=codec)

    if segmented:
        return muxed, l1_filtering_done

    if args.output_format == "SDR":
        dves_bitstream = muxed
    else:
        dves_bitstream = metadata_postproc(
            muxed, args,
            codec=codec,
            rpu_compression=False,
            L1_filter=not l1_filtering_done,
        )

    if not args.dry_run:
        print("Wrote", dves_bitstream)


if __name__ == "__main__":
    try:
        main()
    except Error as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
