const ElvOAction = require("../o-action").ElvOAction;
const ElvOMutex = require("../o-mutex");
const fs = require("fs");
const Path = require("path");

// Mnemonic ID rule (confirmed against "Gone with the Wind" -> GONEWND):
//   - strip stopwords (articles/prepositions), keep the first significant word whole
//     (truncated only if longer than first_word_max_length),
//   - compress every subsequent significant word by dropping vowels (keeping its
//     first letter so it doesn't disappear), and concatenate,
//   - a title with a single significant word (e.g. "Creed", "1984") is used as-is,
//     truncated only if it exceeds max_length.
// Season/episode suffix width (SSEE, e.g. WANDER02 for season 2, WANDER0205 for
// s02e05) was not pinned down precisely in discussion - this uses a plain 2-digit
// season + 2-digit episode, zero-padded, as the simplest consistent option; adjust
// generateSeason/generateEpisode below if a different width is wanted.
//
// Unnumbered seasons/episodes: some series have a season identified by a name
// instead of a number (or no season identifier at all), and/or episodes identified
// by a title instead of a number:
//   - season_number XOR season_name (never both): a name-based season is
//     mnemonic-compressed (same algorithm as titles, just to name_suffix_max_length)
//     and appended to the series code, exactly like a numbered season would be;
//     if NEITHER is given, the season is treated as an implicit single season and
//     contributes no suffix at all (the "season code" is just the series code).
//   - episode_number XOR episode_title (never both, and at least one is required):
//     same treatment - a title-based episode is mnemonic-compressed and appended
//     to the season code.
// Unlike numeric suffixes (collision-free by construction), name-based suffixes can
// collide (two episode titles compressing the same way), so they go through the same
// resolveFreeCode disambiguation used for movie/series titles.
const STOPWORDS = new Set([
    "A", "AN", "THE", "OF", "WITH", "AND", "OR", "IN", "ON", "FOR", "TO", "AT", "BY", "FROM"
]);

class ElvOActionIdGenerator extends ElvOAction {
    
    ActionId() {
        return "id_generator";
    };
    
    Parameters() {
        return {
            parameters: {
                action: {type: "string", values: ["GENERATE_ID", "QUICK_SUM"], required: true}
            }
        };
    };
    
    IOs(parameters) {
        let inputs, outputs;
        if (parameters.action == "GENERATE_ID") {
            inputs = {
                title_type: {type: "string", required: true, values: ["movie", "series", "season", "episode"]},
                title: {type: "string", required: false},
                series_name: {type: "string", required: false},
                season_number: {type: "numeric", required: false},
                season_name: {type: "string", required: false},
                episode_number: {type: "numeric", required: false},
                episode_title: {type: "string", required: false},
                repository_path: {type: "string", required: true},
                max_length: {type: "numeric", required: false, default: 12},
                first_word_max_length: {type: "numeric", required: false, default: 6},
                name_suffix_max_length: {type: "numeric", required: false, default: 4}
            };
            outputs = {
                id: {type: "string"},
                created: {type: "boolean"}
            };
        }
        if (parameters.action == "QUICK_SUM") {
            inputs = {value: {type: "password", required: true}};
            outputs = {quick_sum: {type: "string"}};
        }
        return {inputs, outputs};
    };
    
    async Execute(inputs, outputs) {
        try {
            if (this.Payload.parameters.action == "GENERATE_ID") {
                return await this.executeGenerateId(inputs, outputs);
            }
            if (this.Payload.parameters.action == "QUICK_SUM") {
                return await this.executeQuickSum(inputs, outputs);
            }
        } catch (err) {
            this.Error("action_id_generator failed", err);
            return ElvOAction.EXECUTION_EXCEPTION;
        }
    };
    
    async QuickSum(str) {
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            hash = (Math.imul(31, hash) + str.charCodeAt(i)) | 0;
        } 
        return (hash >>> 0).toString(16).padStart(8, "0");
    } 
    
    // Framework-independent entry point: does the full validate -> lock -> load ->
    // generate-or-reuse -> save -> unlock cycle and returns {id, created}. Callable
    // directly from any module (`ElvOActionIdGenerator.GenerateId({...})`), with no
    // dependency on an ElvOAction instance/job context.
    static async GenerateId(inputs) {
        let titleType = inputs.title_type;
        let maxLength = inputs.max_length || 12;
        let firstWordMaxLength = inputs.first_word_max_length || 6;
        let nameSuffixMaxLength = inputs.name_suffix_max_length || 4;
        
        let hasSeasonNumber = (inputs.season_number !== null && inputs.season_number !== undefined);
        let hasSeasonName = !!(inputs.season_name && inputs.season_name.trim());
        let hasEpisodeNumber = (inputs.episode_number !== null && inputs.episode_number !== undefined);
        let hasEpisodeTitle = !!(inputs.episode_title && inputs.episode_title.trim());
        
        if ((titleType == "movie" || titleType == "series") && !inputs.title) {
            throw new Error("title is required when title_type is '" + titleType + "'");
        }
        if ((titleType == "season" || titleType == "episode") && !inputs.series_name) {
            throw new Error("series_name is required when title_type is '" + titleType + "'");
        }
        if (hasSeasonNumber && hasSeasonName) {
            throw new Error("Provide at most one of season_number or season_name, not both");
        }
        if (hasEpisodeNumber && hasEpisodeTitle) {
            throw new Error("Provide at most one of episode_number or episode_title, not both");
        }
        if (titleType == "episode" && !hasEpisodeNumber && !hasEpisodeTitle) {
            throw new Error("episode_number or episode_title is required when title_type is 'episode'");
        }
        
        let mutex = await ElvOMutex.WaitForLock({name: inputs.repository_path, holdTimeout: 60000, waitTimeout: 60000});
        if (!mutex) {
            throw new Error("Could not acquire mutex for repository: " + inputs.repository_path);
        }
        try {
            let registry = ElvOActionIdGenerator.loadRegistry(inputs.repository_path);
            let result;
            if (titleType == "movie") {
                result = ElvOActionIdGenerator.getOrCreateCode(inputs.title, "movie", registry, maxLength, firstWordMaxLength);
            } else if (titleType == "series") {
                result = ElvOActionIdGenerator.getOrCreateCode(inputs.title, "series", registry, maxLength, firstWordMaxLength);
            } else if (titleType == "season") {
                result = ElvOActionIdGenerator.generateSeason(inputs.series_name, inputs.season_number, inputs.season_name, registry, maxLength, firstWordMaxLength, nameSuffixMaxLength);
            } else if (titleType == "episode") {
                result = ElvOActionIdGenerator.generateEpisode(inputs.series_name, inputs.season_number, inputs.season_name, inputs.episode_number, inputs.episode_title, registry, maxLength, firstWordMaxLength, nameSuffixMaxLength);
            }
            ElvOActionIdGenerator.saveRegistry(inputs.repository_path, registry);
            return {id: result.code, created: result.created};
        } finally {
            ElvOMutex.ReleaseSync(mutex);
        }
    };
    
    async executeGenerateId(inputs, outputs) {
        let result = await ElvOActionIdGenerator.GenerateId(inputs);
        outputs.id = result.id;
        outputs.created = result.created;
        this.ReportProgress(result.created ? "Created new id" : "Reused existing id", result.id);
        return ElvOAction.EXECUTION_COMPLETE;
    };

    async executeQuickSum(inputs, outputs) {
        outputs.quick_sum =  ElvOActionIdGenerator.QuickSum(inputs.value);
        return ElvOAction.EXECUTION_COMPLETE;
    };
    
    // --- registry persistence ---
    
    static loadRegistry(repositoryPath) {
        if (!fs.existsSync(repositoryPath)) {
            return {codes: {}};
        }
        let raw = fs.readFileSync(repositoryPath, "utf8");
        if (!raw || !raw.trim()) return {codes: {}};
        let parsed = JSON.parse(raw);
        if (!parsed.codes) parsed.codes = {};
        return parsed;
    };
    
    static saveRegistry(repositoryPath, registry) {
        let dir = Path.dirname(repositoryPath);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, {recursive: true});
        let tmpPath = repositoryPath + ".tmp." + process.pid;
        fs.writeFileSync(tmpPath, JSON.stringify(registry, null, 2));
        fs.renameSync(tmpPath, repositoryPath);
    };
    
    // --- id generation ---
    
    static normalizeWords(title) {
        let cleaned = title.replace(/['’]/g, "");
        cleaned = cleaned.replace(/[^A-Za-z0-9]+/g, " ");
        return cleaned.trim().split(/\s+/).filter(w => w.length > 0).map(w => w.toUpperCase());
    };
    
    static significantWords(title) {
        let words = ElvOActionIdGenerator.normalizeWords(title);
        let significant = words.filter(w => !STOPWORDS.has(w));
        if (significant.length == 0) significant = words;
        if (significant.length == 0) throw new Error("Title contains no usable characters: " + title);
        return significant;
    };
    
    static compressWord(word) {
        if (word.length <= 1) return word;
        return word[0] + word.slice(1).replace(/[AEIOU]/g, "");
    };
    
    static generateBaseCode(title, maxLength, firstWordMaxLength) {
        let words = ElvOActionIdGenerator.significantWords(title);
        let code;
        if (words.length == 1) {
            code = words[0];
        } else {
            let first = words[0].length > firstWordMaxLength ? words[0].slice(0, firstWordMaxLength) : words[0];
            let rest = words.slice(1).map(w => ElvOActionIdGenerator.compressWord(w)).join("");
            code = first + rest;
        }
        if (code.length > maxLength) code = code.slice(0, maxLength);
        return code;
    };
    
    static pad(number, width) {
        return String(number).padStart(width, "0");
    };
    
    // get-or-create a code for a movie or series title, keyed by (title_type, normalized title)
    // so re-generating for the same title is idempotent instead of colliding with itself.
    static getOrCreateCode(title, titleType, registry, maxLength, firstWordMaxLength) {
        let identityKey = titleType + "|" + title.trim().toLowerCase();
        for (let code in registry.codes) {
            if (registry.codes[code].identity_key == identityKey) return {code, created: false};
        }
        let base = ElvOActionIdGenerator.generateBaseCode(title, maxLength, firstWordMaxLength);
        let code = ElvOActionIdGenerator.resolveFreeCode(base, registry, maxLength);
        registry.codes[code] = {
            title_type: titleType, title: title, identity_key: identityKey, created_at: (new Date()).toISOString()
        };
        return {code, created: true};
    };
    
    // the mnemonic algorithm is deterministic but not collision-free (e.g. two unrelated
    // titles compressing to the same code); disambiguate with an incrementing numeric
    // suffix, trimming the base if needed to respect max_length.
    static resolveFreeCode(baseCode, registry, maxLength) {
        let candidate = baseCode;
        let suffix = 1;
        while (registry.codes[candidate]) {
            suffix++;
            let suffixStr = String(suffix);
            let trimmedBase = baseCode;
            if ((baseCode.length + suffixStr.length) > maxLength) {
                trimmedBase = baseCode.slice(0, Math.max(1, maxLength - suffixStr.length));
            }
            candidate = trimmedBase + suffixStr;
            if (suffix > 999) throw new Error("Could not resolve a unique id for base code: " + baseCode);
        }
        return candidate;
    };
    
    // season_number XOR season_name; if neither is given, this series has a single
    // implicit season, so the "season code" is just the series code (no registry
    // entry created, no suffix). A named season is mnemonic-compressed and
    // disambiguated on collision like a movie/series title, since (unlike a numeric
    // suffix) two different season names can compress to the same short code.
    static generateSeason(seriesName, seasonNumber, seasonName, registry, maxLength, firstWordMaxLength, nameSuffixMaxLength) {
        let hasNumber = (seasonNumber !== null && seasonNumber !== undefined);
        let hasName = !!(seasonName && seasonName.trim());
        let seriesResult = ElvOActionIdGenerator.getOrCreateCode(seriesName, "series", registry, maxLength, firstWordMaxLength);
        
        if (!hasNumber && !hasName) {
            return {code: seriesResult.code, created: seriesResult.created, seriesCode: seriesResult.code};
        }
        
        let identityKey = "season|" + seriesName.trim().toLowerCase() + "|" +
        (hasNumber ? "n:" + seasonNumber : "s:" + seasonName.trim().toLowerCase());
        for (let code in registry.codes) {
            if (registry.codes[code].identity_key == identityKey) return {code, created: false, seriesCode: seriesResult.code};
        }
        
        let code;
        if (hasNumber) {
            code = seriesResult.code + ElvOActionIdGenerator.pad(seasonNumber, 2);
            if (registry.codes[code]) {
                throw new Error("Generated season id collides with an existing unrelated entry: " + code);
            }
        } else {
            let suffix = ElvOActionIdGenerator.generateBaseCode(seasonName, nameSuffixMaxLength, nameSuffixMaxLength);
            code = ElvOActionIdGenerator.resolveFreeCode(seriesResult.code + suffix, registry, maxLength);
        }
        registry.codes[code] = {
            title_type: "season", series_name: seriesName,
            season_number: hasNumber ? seasonNumber : null, season_name: hasName ? seasonName : null,
            series_code: seriesResult.code, identity_key: identityKey, created_at: (new Date()).toISOString()
        };
        return {code, created: true, seriesCode: seriesResult.code};
    };
    
    // episode_number XOR episode_title (one is required, enforced by the caller).
    // The season component is resolved via generateSeason so it stays consistent
    // (same code, same disambiguation) across every episode of the same season,
    // whether that season is numbered, named, or implicit.
    static generateEpisode(seriesName, seasonNumber, seasonName, episodeNumber, episodeTitle, registry, maxLength, firstWordMaxLength, nameSuffixMaxLength) {
        let hasEpisodeNumber = (episodeNumber !== null && episodeNumber !== undefined);
        let hasEpisodeTitle = !!(episodeTitle && episodeTitle.trim());
        if (!hasEpisodeNumber && !hasEpisodeTitle) {
            throw new Error("episode_number or episode_title is required when title_type is 'episode'");
        }
        
        let seasonResult = ElvOActionIdGenerator.generateSeason(seriesName, seasonNumber, seasonName, registry, maxLength, firstWordMaxLength, nameSuffixMaxLength);
        
        let hasSeasonNumber = (seasonNumber !== null && seasonNumber !== undefined);
        let hasSeasonName = !!(seasonName && seasonName.trim());
        let seasonIdentityPart = hasSeasonNumber ? "n:" + seasonNumber
        : (hasSeasonName ? "s:" + seasonName.trim().toLowerCase() : "none");
        
        let identityKey = "episode|" + seriesName.trim().toLowerCase() + "|" + seasonIdentityPart + "|" +
        (hasEpisodeNumber ? "n:" + episodeNumber : "t:" + episodeTitle.trim().toLowerCase());
        for (let code in registry.codes) {
            if (registry.codes[code].identity_key == identityKey) return {code, created: false};
        }
        
        let code;
        if (hasEpisodeNumber) {
            code = seasonResult.code + ElvOActionIdGenerator.pad(episodeNumber, 2);
            if (registry.codes[code]) {
                throw new Error("Generated episode id collides with an existing unrelated entry: " + code);
            }
        } else {
            let suffix = ElvOActionIdGenerator.generateBaseCode(episodeTitle, nameSuffixMaxLength, nameSuffixMaxLength);
            code = ElvOActionIdGenerator.resolveFreeCode(seasonResult.code + suffix, registry, maxLength);
        }
        registry.codes[code] = {
            title_type: "episode", series_name: seriesName,
            season_number: hasSeasonNumber ? seasonNumber : null, season_name: hasSeasonName ? seasonName : null,
            episode_number: hasEpisodeNumber ? episodeNumber : null, episode_title: hasEpisodeTitle ? episodeTitle : null,
            series_code: seasonResult.seriesCode, season_code: seasonResult.code,
            identity_key: identityKey, created_at: (new Date()).toISOString()
        };
        return {code, created: true};
    };
    
    static VERSION_HISTORY = {
        "0.0.1": "2026-09-02 - Initial version",
        "0.0.2": "2026-09-24 - Supports seasons identified by season_name instead of season_number (or neither, for a single implicit season), and episodes identified by episode_title instead of episode_number",
        "0.0.3": "2026-09-25 - Moves all id-generation logic into a static GenerateId() method, callable directly from other modules without the ElvOAction framework; executeGenerateId is now a thin wrapper around it"
    };
    static VERSION = "0.0.3";
};

if (require.main === module) {
    ElvOAction.Run(ElvOActionIdGenerator);
} else {
    module.exports = ElvOActionIdGenerator;
}
