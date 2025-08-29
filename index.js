const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const ffmpeg = require('fluent-ffmpeg');
const path = require('path');
const fs = require('fs');

const app = express();
const port = process.env.PORT || 3001;

// Initialize the Supabase Admin Client
// Ensure these environment variables are set in your Render dashboard
const supabaseAdmin = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

app.use(express.json());

// Main transcoding endpoint
app.post('/transcode', async (req, res) => {
  const { videoId } = req.body;
  if (!videoId) {
    return res.status(400).send({ error: 'videoId is required' });
  }

  // Acknowledge the request immediately so the frontend doesn't time out
  res.status(202).send({ message: `Accepted. Processing video: ${videoId}` });

  // --- Start the long-running process in the background ---
  try {
    // Step 1: Get video metadata from the database
    const { data: videoData, error: dbError } = await supabaseAdmin
      .from('videos')
      .select('raw_path')
      .eq('id', videoId)
      .single();

    if (dbError || !videoData) {
      throw new Error(`Video not found in DB for ID: ${videoId}`);
    }

    // Create temporary local directories for processing
    const tempRawDir = path.join(__dirname, 'temp_raw');
    const tempHlsDir = path.join(__dirname, 'temp_hls');
    fs.mkdirSync(tempRawDir, { recursive: true });
    fs.mkdirSync(tempHlsDir, { recursive: true });

    const localRawPath = path.join(tempRawDir, videoData.raw_path);
    const localHlsPlaylistPath = path.join(tempHlsDir, 'playlist.m3u8');
    const localThumbnailPath = path.join(tempOutputDir, 'thumbnail.png');
    const thumbnailFileName = `${videoId}.png`;

    // Step 2: Download the raw video file from Supabase Storage
    console.log(`Downloading raw file: ${videoData.raw_path}`);
    const { data: fileData, error: downloadError } = await supabaseAdmin.storage
      .from('raw_uploads')
      .download(videoData.raw_path);

    if (downloadError) throw downloadError;
    fs.writeFileSync(localRawPath, Buffer.from(await fileData.arrayBuffer()));
    console.log(`Downloaded to: ${localRawPath}`);


    console.log('Generating thumbnail...');
    await new Promise((resolve, reject) => {
      ffmpeg(localRawPath)
        .screenshots({
          timestamps: ['00:00:02'], // Take thumbnail at the 2-second mark
          filename: 'thumbnail.png',
          folder: tempOutputDir,
          size: '640x360'
        })
        .on('end', resolve)
        .on('error', reject);
    });
    console.log('Thumbnail generated.');

    // Step 3: Run FFMPEG to transcode the video to HLS
    console.log(`Starting transcoding for video: ${videoId}`);
    await new Promise((resolve, reject) => {
      ffmpeg(localRawPath)
        .outputOptions([
          '-c:v h264',       // Use the H.264 video codec
          '-hls_time 10',     // Create 10-second video segments
          '-hls_list_size 0', // Keep all segments in the playlist
          '-f hls'            // The output format is HLS
        ])
        .output(localHlsPlaylistPath)
        .on('end', resolve) // Resolve the promise when transcoding is finished
        .on('error', (err) => reject(new Error(err))) // Reject on error
        .run();
    });
    console.log('Transcoding finished.');


    const thumbnailBuffer = fs.readFileSync(localThumbnailPath);
    console.log(`Uploading thumbnail: ${thumbnailFileName}`);
    const { error: thumbUploadError } = await supabaseAdmin.storage
        .from('thumbnails')
        .upload(thumbnailFileName, thumbnailBuffer, { contentType: 'image/png' });
    if (thumbUploadError) throw thumbUploadError;

    // Step 4: Upload the HLS files to Supabase Storage
    const hlsFiles = fs.readdirSync(tempHlsDir);
    for (const file of hlsFiles) {
      const filePath = path.join(tempHlsDir, file);
      const fileBuffer = fs.readFileSync(filePath);
      console.log(`Uploading HLS file: ${file}`);
      const { error: uploadError } = await supabaseAdmin.storage
        .from('hls') // Your HLS bucket
        .upload(`${videoId}/${file}`, fileBuffer, {
          // Set the correct content type for the files
          contentType: file.endsWith('.m3u8') ? 'application/vnd.apple.mpegurl' : 'video/MP2T',
          upsert: true // Overwrite if files already exist
        });
      if (uploadError) throw uploadError;
    }
    console.log('HLS upload complete.');

    // Step 5: Update the database record to 'ready'
    await supabaseAdmin
      .from('videos')
      .update({
        status: 'ready',
        hls_path: `${videoId}/playlist.m3u8`,
        thumbnail_path: thumbnailFileName
      })
      .eq('id', videoId);

    console.log(`✅ Video ${videoId} is ready!`);

  } catch (error) {
    console.error(`❌ Failed to process video ${videoId}:`, error);
    // If anything fails, update the status to 'failed'
    await supabaseAdmin
      .from('videos')
      .update({ status: 'failed' })
      .eq('id', videoId);

  } finally {
    // Step 6: Cleanup - always delete the temporary local files
    const tempRawDir = path.join(__dirname, 'temp_raw');
    const tempHlsDir = path.join(__dirname, 'temp_hls');
    fs.rmSync(tempRawDir, { recursive: true, force: true });
    fs.rmSync(tempHlsDir, { recursive: true, force: true });
    console.log('Cleanup complete.');
  }
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});