const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const ffmpeg = require('fluent-ffmpeg');
const path = require('path');
const fs = require('fs');

const app = express();
const port = process.env.PORT || 3001;

// Initialize the Supabase Admin Client
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

  // Acknowledge the request immediately
  res.status(202).send({ message: `Accepted. Processing video: ${videoId}` });

  // --- Start the long-running process in the background ---
  try {
    const { data: videoData, error: dbError } = await supabaseAdmin
      .from('videos')
      .select('raw_path')
      .eq('id', videoId)
      .single();

    if (dbError || !videoData) {
      throw new Error(`Video not found in DB for ID: ${videoId}`);
    }

    const tempRawDir = path.join(__dirname, 'temp_raw');
    const tempOutputDir = path.join(__dirname, 'temp_output');
    fs.mkdirSync(tempRawDir, { recursive: true });
    fs.mkdirSync(tempOutputDir, { recursive: true });
    
    // Create subdirectories for each resolution
    fs.mkdirSync(path.join(tempOutputDir, '720p'), { recursive: true });
    fs.mkdirSync(path.join(tempOutputDir, '480p'), { recursive: true });
    fs.mkdirSync(path.join(tempOutputDir, '240p'), { recursive: true });

    const localRawPath = path.join(tempRawDir, videoData.raw_path);
    const thumbnailFileName = `${videoId}.png`;
    const localThumbnailPath = path.join(tempOutputDir, 'thumbnail.png');

    // Download raw file
    console.log(`Downloading raw file: ${videoData.raw_path}`);
    const { data: fileData, error: downloadError } = await supabaseAdmin.storage
      .from('raw_uploads')
      .download(videoData.raw_path);
    if (downloadError) throw downloadError;
    fs.writeFileSync(localRawPath, Buffer.from(await fileData.arrayBuffer()));
    console.log(`Downloaded to: ${localRawPath}`);

    // Generate Thumbnail
    console.log('Generating thumbnail...');
    await new Promise((resolve, reject) => {
      ffmpeg(localRawPath)
        .screenshots({
          timestamps: ['00:00:02'],
          filename: 'thumbnail.png',
          folder: tempOutputDir,
          size: '640x360'
        })
        .on('end', resolve)
        .on('error', reject);
    });
    console.log('Thumbnail generated.');

    // --- UPDATED: Run FFMPEG to transcode to 720p, 480p, and 240p ---
    console.log('Starting Adaptive Bitrate (ABR) transcoding...');
    await new Promise((resolve, reject) => {
      ffmpeg(localRawPath)
        .outputOptions([
          // Define stream mappings and codecs
          '-map 0:v:0 -map 0:a:0 -map 0:v:0 -map 0:a:0 -map 0:v:0 -map 0:a:0',
          
          // Filter to create three scaled video outputs
          '-filter:v:0 scale=-2:720', '-c:v:0 libx264', '-b:v:0 2000k', // 720p stream
          '-filter:v:1 scale=-2:480', '-c:v:1 libx264', '-b:v:1 800k',  // 480p stream
          '-filter:v:2 scale=-2:240', '-c:v:2 libx264', '-b:v:2 400k',  // 240p stream
          '-c:a aac', '-b:a 128k', // Audio codec for all streams

          // HLS options for ABR
          '-f hls',
          '-hls_time 10',
          '-hls_playlist_type vod',
          '-hls_segment_filename', `${tempOutputDir}/%v/segment%03d.ts`,
          '-master_pl_name playlist.m3u8',
          '-var_stream_map', "v:0,a:0,name:720p v:1,a:1,name:480p v:2,a:2,name:240p"
        ])
        .output(`${tempOutputDir}/%v/playlist.m3u8`)
        .on('end', resolve)
        .on('error', (err) => reject(new Error(err)))
        .run();
    });
    console.log('ABR transcoding finished.');

    // --- Upload HLS files and Thumbnail in Parallel ---
    console.log('Starting parallel uploads...');
    const filesToUpload = [];
    filesToUpload.push({ name: 'playlist.m3u8', path: path.join(tempOutputDir, 'playlist.m3u8')});
    fs.readdirSync(path.join(tempOutputDir, '720p')).forEach(f => filesToUpload.push({ name: `720p/${f}`, path: path.join(tempOutputDir, '720p', f)}));
    fs.readdirSync(path.join(tempOutputDir, '480p')).forEach(f => filesToUpload.push({ name: `480p/${f}`, path: path.join(tempOutputDir, '480p', f)}));
    fs.readdirSync(path.join(tempOutputDir, '240p')).forEach(f => filesToUpload.push({ name: `240p/${f}`, path: path.join(tempOutputDir, '240p', f)}));

    const uploadPromises = filesToUpload.map(file => {
      const fileBuffer = fs.readFileSync(file.path);
      const contentType = file.name.endsWith('.m3u8') ? 'application/vnd.apple.mpegurl' : 'video/MP2T';
      return supabaseAdmin.storage
        .from('hls')
        .upload(`${videoId}/${file.name}`, fileBuffer, { contentType, upsert: true });
    });
    
    const thumbnailBuffer = fs.readFileSync(localThumbnailPath);
    uploadPromises.push(supabaseAdmin.storage.from('thumbnails').upload(thumbnailFileName, thumbnailBuffer, { contentType: 'image/png' }));
    
    const uploadResults = await Promise.all(uploadPromises);
    const uploadError = uploadResults.find(result => result.error);
    if (uploadError) throw uploadError.error;
    console.log('All uploads complete.');

    // Update the database record
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
    await supabaseAdmin
      .from('videos')
      .update({ status: 'failed' })
      .eq('id', videoId);
  } finally {
    fs.rmSync(path.join(__dirname, 'temp_raw'), { recursive: true, force: true });
    fs.rmSync(path.join(__dirname, 'temp_output'), { recursive: true, force: true });
    console.log('Cleanup complete.');
  }
});

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});