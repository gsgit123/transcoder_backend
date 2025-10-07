const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const ffmpeg = require('fluent-ffmpeg');
const ffmpegPath = require('ffmpeg-static'); // Use static ffmpeg
const path = require('path');
const fs = require('fs');

const app = express();
const port = process.env.PORT || 3001;

ffmpeg.setFfmpegPath(ffmpegPath);

const supabaseAdmin = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

app.use(express.json());

// Avoid 502 on favicon
app.get('/favicon.ico', (req, res) => res.status(204).end());

// Health check
app.get("/", (req, res) => {
  res.send(`Transcoding service running on port ${port}`);
});

// Transcoding endpoint
app.post('/transcode', async (req, res) => {
  const { videoId } = req.body;
  if (!videoId) return res.status(400).send({ error: 'videoId is required' });

  // Respond immediately
  res.status(202).send({ message: `Accepted. Processing video: ${videoId}` });

  try {
    // Get video info from Supabase
    const { data: videoData, error: dbError } = await supabaseAdmin
      .from('videos')
      .select('raw_path')
      .eq('id', videoId)
      .single();

    if (dbError || !videoData) throw new Error(`Video not found for ID: ${videoId}`);

    // Temp directories in /tmp
    const tempRawDir = path.join('/tmp', 'temp_raw');
    const tempHlsDir = path.join('/tmp', 'temp_hls');
    fs.mkdirSync(tempRawDir, { recursive: true });
    fs.mkdirSync(tempHlsDir, { recursive: true });

    const localRawPath = path.join(tempRawDir, videoData.raw_path);
    const localHlsPlaylistPath = path.join(tempHlsDir, 'playlist.m3u8');
    const localThumbnailPath = path.join(tempHlsDir, 'thumbnail.png');
    const thumbnailFileName = `${videoId}.png`;

    // Download raw video
    const { data: fileData, error: downloadError } = await supabaseAdmin.storage
      .from('raw_uploads')
      .download(videoData.raw_path);

    if (downloadError) throw downloadError;
    fs.writeFileSync(localRawPath, Buffer.from(await fileData.arrayBuffer()));

    // Generate thumbnail
    await new Promise((resolve, reject) => {
      ffmpeg(localRawPath)
        .screenshots({
          timestamps: ['00:00:02'],
          filename: 'thumbnail.png',
          folder: tempHlsDir,
          size: '640x360'
        })
        .on('end', resolve)
        .on('error', reject);
    });

    // Transcode to HLS
    await new Promise((resolve, reject) => {
      ffmpeg(localRawPath)
        .outputOptions(['-c:v h264', '-hls_time 10', '-hls_list_size 0', '-f hls'])
        .output(localHlsPlaylistPath)
        .on('end', resolve)
        .on('error', reject)
        .run();
    });

    // Upload thumbnail
    const thumbnailBuffer = fs.readFileSync(localThumbnailPath);
    const { error: thumbUploadError } = await supabaseAdmin.storage
      .from('thumbnails')
      .upload(thumbnailFileName, thumbnailBuffer, { contentType: 'image/png' });
    if (thumbUploadError) throw thumbUploadError;

    // Upload HLS files
    const hlsFiles = fs.readdirSync(tempHlsDir);
    for (const file of hlsFiles) {
      const fileBuffer = fs.readFileSync(path.join(tempHlsDir, file));
      const { error: uploadError } = await supabaseAdmin.storage
        .from('hls')
        .upload(`${videoId}/${file}`, fileBuffer, {
          contentType: file.endsWith('.m3u8') ? 'application/vnd.apple.mpegurl' : 'video/MP2T',
          upsert: true
        });
      if (uploadError) throw uploadError;
    }

    // Update DB
    await supabaseAdmin.from('videos').update({
      status: 'ready',
      hls_path: `${videoId}/playlist.m3u8`,
      thumbnail_path: thumbnailFileName
    }).eq('id', videoId);

    console.log(`✅ Video ${videoId} ready!`);

  } catch (error) {
    console.error(`❌ Failed video ${videoId}:`, error);
    await supabaseAdmin.from('videos').update({ status: 'failed' }).eq('id', videoId);

  } finally {
    // Cleanup
    fs.rmSync('/tmp/temp_raw', { recursive: true, force: true });
    fs.rmSync('/tmp/temp_hls', { recursive: true, force: true });
    console.log('Cleanup complete.');
  }
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
