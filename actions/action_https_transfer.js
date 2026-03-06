const ElvOAction = require("../o-action").ElvOAction
const fs = require("fs")
const Path = require("path")
const axios = require("axios")
const FormData = require("form-data")

class ElvOActionHttpTransfer extends ElvOAction {

  ActionId() {
    return "https_transfer"
  }

  Parameters() {
    return {
      "parameters": {
        action: { type: "string", values: ["UPLOAD", "DOWNLOAD", "DOWNLOAD_FILE"] },
      },
    }
  }

  IOs(parameters) {
    let inputs = {      
      target_flattening_base: { type: "string", required: false, default: null },
      headers : { type: "object", required: false, default: {} }
    }

    let outputs = {}

    if (parameters.action === "UPLOAD") {
      inputs.url = { type: "string", required: true }
      inputs.local_files_path = { type: "array", required: true }     
      outputs.uploaded_files = "object"
    }

    if (parameters.action === "DOWNLOAD") {
      inputs.remote_files_path = { type: "array", required: true }
      inputs.target_folder = { type: "string", required: true }
      inputs.rename_files = { type: "object", required: false, default: {} }
      outputs.downloaded_files = "object"
    }

    if (parameters.action === "DOWNLOAD_FILE") {
      inputs.remote_file_path = { type: "string", required: true }
      inputs.target_file_name = { type: "string", required: false, default: null }
      inputs.target_folder = { type: "string", required: true }      
      outputs.downloaded_file = "string"
    }

    return { inputs: inputs, outputs: outputs }
  }

  flatten(sourceFilePath, base) {
    if (!base) return Path.basename(sourceFilePath)
    return sourceFilePath.replace(base, "")
  }

  async executeHttpUpload(inputs, outputs) {
    outputs.uploaded_files = {}

    for (let filePath of inputs.local_files_path) {
      try {
        this.ReportProgress("Uploading " + filePath)
        const form = new FormData()
        form.append("file", fs.createReadStream(filePath))

        const response = await axios.post(inputs.url, form, {
          headers: {
            ...inputs.headers,
            ...form.getHeaders(),
          },
        })

        outputs.uploaded_files[filePath] = response.data
      } catch (err) {
        this.Error("Failed to upload " + filePath, err)
      }
    }

    this.ReportProgress("Upload complete")
    return ElvOAction.EXECUTION_COMPLETE
  }

  async executeHttpDownload(inputs, outputs) {   outputs.downloaded_files = {}

    for (let remoteUrl of inputs.remote_files_path) {
      try {
        this.ReportProgress("Checking " + remoteUrl)

        const headResponse = await axios.head(remoteUrl, {
          headers: inputs.headers,
        })

        const remoteFileSize = parseInt(headResponse.headers["content-length"])
        const baseName = Path.basename(remoteUrl.split("?")[0])
        const flattenedName = this.flatten(baseName, inputs.target_flattening_base)
        const renamed = inputs.rename_files?.[remoteUrl] || flattenedName
        const targetFilePath = Path.join(inputs.target_folder, renamed)

        if (fs.existsSync(targetFilePath)) {
          const localFileSize = fs.statSync(targetFilePath).size
          if (localFileSize === remoteFileSize) {
            this.ReportProgress(`Skipping ${remoteUrl} (already downloaded with matching size)`)
            outputs.downloaded_files[remoteUrl] = targetFilePath
            continue
          }
        }

        this.ReportProgress("Downloading from " + remoteUrl)
        const response = await axios.get(remoteUrl, {
          responseType: "stream",
          headers: inputs.headers,
        })

        const writer = fs.createWriteStream(targetFilePath)
        response.data.pipe(writer)

        await new Promise((resolve, reject) => {
          writer.on("finish", resolve)
          writer.on("error", reject)
        })

        outputs.downloaded_files[remoteUrl] = targetFilePath
      } catch (err) {
        this.Error("Failed to download from " + remoteUrl, err)
      }
    }

    this.ReportProgress("Download complete")
    return ElvOAction.EXECUTION_COMPLETE
  }

  async executeHttpFileDownload(inputs, outputs) {
    try {
      this.ReportProgress("Checking " + inputs.remote_file_path)

      const headResponse = await axios.head(inputs.remote_file_path, {
        headers: inputs.headers,
      })

      const remoteFileSize = parseInt(headResponse.headers["content-length"])
      const fileName = inputs.target_file_name || Path.basename(inputs.remote_file_path.split("?")[0])
      const targetFilePath = Path.join(inputs.target_folder, fileName)

      if (fs.existsSync(targetFilePath)) {
        const localFileSize = fs.statSync(targetFilePath).size
        if (localFileSize === remoteFileSize) {
          this.ReportProgress(`Skipping ${inputs.remote_file_path} (already downloaded with matching size)`)
          outputs.downloaded_file = targetFilePath
          return ElvOAction.EXECUTION_COMPLETE
        }
      }

      this.ReportProgress("Downloading file from " + inputs.remote_file_path)

      const response = await axios.get(inputs.remote_file_path, {
        responseType: "stream",
        headers: inputs.headers,
      })

      const writer = fs.createWriteStream(targetFilePath)
      response.data.pipe(writer)

      await new Promise((resolve, reject) => {
        writer.on("finish", resolve)
        writer.on("error", reject)
      })

      outputs.downloaded_file = targetFilePath
      this.ReportProgress("Download complete")
      return ElvOAction.EXECUTION_COMPLETE
    } catch (err) {
      this.Error("Failed to download file", err)
      return ElvOAction.EXECUTION_EXCEPTION
    }
  }

  async Execute(inputs, outputs) {
    try {
      if (this.Payload.parameters.action === "UPLOAD") {
        return await this.executeHttpUpload(this.Payload.inputs, outputs)
      }
      if (this.Payload.parameters.action === "DOWNLOAD") {
        return await this.executeHttpDownload(this.Payload.inputs, outputs)
      }
      if (this.Payload.parameters.action === "DOWNLOAD_FILE") {
        return await this.executeHttpFileDownload(this.Payload.inputs, outputs)
      }
      throw "Unsupported action: " + this.Payload.parameters.action
    } catch (err) {
      this.Error("Execution error", err)
      return ElvOAction.EXECUTION_EXCEPTION
    }
  }

  static VERSION = "0.3.2"
  static REVISION_HISTORY = {
    "0.1.0": "Initial release for HTTPS-based upload and download",
    "0.2.0": "Added support for authentication headers, file flattening, and renaming",
    "0.3.0": "Added DOWNLOAD_FILE support with optional renaming",
    "0.3.1": "Fixed missing headers input for some actions",
    "0.3.2": "Added logic to check if target file already exists and it has the same size, skipping download if so",
  }
}

if (ElvOAction.executeCommandLine(ElvOActionHttpTransfer)) {
  ElvOAction.Run(ElvOActionHttpTransfer)
} else {
  module.exports = ElvOActionHttpTransfer
}