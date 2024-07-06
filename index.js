const path = require("path");
const fs = require("fs");
const express = require("express");
const WebSocket = require("ws");
const { exec } = require('child_process');
const AWS = require('aws-sdk');
const mysql = require('mysql2/promise');
const moment = require('moment-timezone');

require('dotenv').config();

const app = express();

const WS_PORT = process.env.WS_PORT || 443;
const HTTP_PORT = process.env.HTTP_PORT || 8000;

// Configure AWS with environment variables
AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION
});

// Set up S3 client
const s3 = new AWS.S3();

// RDS MySQL configuration
const dbConfig = {
  host: process.env.RDS_HOST,
  user: process.env.RDS_USERNAME,
  password: process.env.RDS_PASSWORD,
  database: process.env.RDS_DB_NAME,
};

// Function to get the latest mission ID from the mission table
const getLatestMissionId = async () => {
  let connection;
  try {
    connection = await mysql.createConnection(dbConfig);
    const [rows] = await connection.execute('SELECT mission_id FROM missions ORDER BY start_date DESC LIMIT 1');
    return rows[0].mission_id;
  } catch (error) {
    console.error('Failed to retrieve mission:', error);
    throw error;
  } finally {
    if (connection) {
      await connection.end();
    }
  }
};

const wsServer = new WebSocket.Server({ port: WS_PORT }, () =>
  console.log(`WS server is listening at ws://192.168.1.140:${WS_PORT}`)
);

// array of connected websocket clients
let connectedClients = [];
let audioBuffer = [];

// Function to process and save audio
async function processAndSaveAudio(audioBuffer) {
  if (audioBuffer.length === 0) {
    console.log('No audio data to process.');
    return;
  }

  const missionId = await getLatestMissionId();
  const tempFilePath = path.join(__dirname, 'tempAudio.raw');
  const outputFilePath = path.join(__dirname, 'output.mp3');

  // Ensure the directory for the output file exists
  if (!fs.existsSync(path.dirname(outputFilePath))) {
    fs.mkdirSync(path.dirname(outputFilePath), { recursive: true });
  }

  // Save the raw audio data to a temporary file
  fs.writeFileSync(tempFilePath, Buffer.concat(audioBuffer));

  // Construct the ffmpeg command
  const ffmpegCommand = `ffmpeg -f s16le -ar 11025 -ac 1 -i "${tempFilePath}" -filter:a "volume=10dB" "${outputFilePath}"`;

  // Execute the ffmpeg command
  exec(ffmpegCommand, (error, stdout, stderr) => {
    if (error) {
      console.error(`exec error: ${error}`);
      return;
    }

    // After successful conversion, upload to S3
    uploadFileToS3(outputFilePath, missionId);
  });
}

// Function to upload file to S3
function uploadFileToS3(filePath, missionId) {
  const fileContent = fs.readFileSync(filePath);
  const bucketName = 'dronesense-audio-bucket';
  const formattedDate = moment().tz('Etc/GMT-3').format('DD-MM-YYYY_HH:mm:ss');
  const key = `${missionId}/audio_${formattedDate}.mp3`;

  const params = {
    Bucket: bucketName,
    Key: key,
    Body: fileContent,
    ContentType: 'audio/mp3'
  };

  s3.upload(params, function(err, data) {
    if (err) {
      console.error("Error uploading data: ", err);
    } else {
      console.log("Successfully uploaded data to " + bucketName + "/" + key);
      // Clean up local files after upload
      fs.unlinkSync(filePath);
      fs.unlinkSync(path.join(__dirname, 'tempAudio.raw'));
    }
  });
}

// Set up a timer to process audio every 20 seconds
setInterval(() => {
  console.log('Processing audio buffer');
  processAndSaveAudio(audioBuffer);
  audioBuffer = [];
}, 20000); // 20 seconds

wsServer.on("connection", (ws, req) => {
  console.log("Connected");
  // add new connected client
  connectedClients.push(ws);
  // listen for messages from the streamer
  ws.on("message", (data) => {
    // Collect incoming audio data
    audioBuffer.push(data);
    // Broadcast data to all connected clients
    connectedClients.forEach((ws, i) => {
      if (ws.readyState === ws.OPEN) {
        ws.send(data);
      } else {
        connectedClients.splice(i, 1);
      }
    });
  });
  ws.on("close", () => {
    console.log("Connection closed");
  });
});

app.use("/image", express.static("image"));
app.use("/js", express.static("js"));
app.get("/audio", (req, res) =>
  res.sendFile(path.resolve(__dirname, "./audio_client.html"))
);

app.listen(HTTP_PORT, () =>
  console.log(`HTTP server listening at http://localhost:${HTTP_PORT}`)
);
