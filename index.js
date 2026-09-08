require("dotenv").config();
const express = require('express');
const { createClient } = require('@supabase/supabase-js');
const ffmpeg = require('fluent-ffmpeg');
const ffmpegPath = require('ffmpeg-static');
const ffprobePath = require('ffprobe-static').path;
const path = require('path');
const fs = require('fs');

const app = express();
const port = process.env.PORT || 3001;

ffmpeg.setFfmpegPath(ffmpegPath);
ffmpeg.setFfprobePath(ffprobePath);

const supabaseAdmin = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

app.use(express.json());
app.get('/favicon.ico', (req, res) => res.status(204).end());

// ─── In-Memory Queue (#1) ─────────────────────────────────────────────────────
// A simple FIFO array. Since this is a single-instance Render service, no Redis
// needed. Solves the concurrent-FFmpeg crash problem with zero extra services.
//
// Lifecycle:
//   POST /transcode → enqueue(videoId) → 202 returned immediately
//   processNext()  → picks one job → runs transcodeVideo() → picks next
//
// On crash: queued jobs are lost, but they were already marked 'processing' in
// the DB, so recoverStuckVideos() marks them 'failed' on the next startup.
// ─────────────────────────────────────────────────────────────────────────────
const jobQueue = [];          // FIFO: [ videoId, videoId, ... ]
let isProcessing = false;     // true while FFmpeg is running

function enqueue(videoId) {
  // Prevent duplicate jobs for the same video
  if (jobQueue.includes(videoId)) {
    console.log(`⚠️  Video ${videoId} already in queue — skipping duplicate.`);
    return;
  }
  jobQueue.push(videoId);
  console.log(`📥 Queued ${videoId}. Queue depth: ${jobQueue.length}`);
  processNext(); // kick off if idle
}

async function processNext() {
  if (isProcessing || jobQueue.length === 0) return;
  isProcessing = true;
  const videoId = jobQueue.shift();
  console.log(`🎬 Processing ${videoId}. Remaining in queue: ${jobQueue.length}`);
  try {
    await transcodeVideo(videoId);
  } catch (err) {
    console.error(`❌ Unhandled error for ${videoId}:`, err.message);
  } finally {
    isProcessing = false;
    processNext(); // pick up next job
  }
}

// ─── Health Endpoint ──────────────────────────────────────────────────────────
app.get("/", (req, res) => {
  const mem = process.memoryUsage();
  res.json({
    status: "ok",
    uptime: Math.floor(process.uptime()),
    queue: {
      depth: jobQueue.length,
      isProcessing,
    },
    memory: {
      rss_mb: Math.round(mem.rss / 1024 / 1024),
      heap_used_mb: Math.round(mem.heapUsed / 1024 / 1024),
      heap_total_mb: Math.round(mem.heapTotal / 1024 / 1024),
    },
  });
});

// ─── Transcode Endpoint ───────────────────────────────────────────────────────
// Now just a thin intake: validates auth, enqueues, returns 202 immediately.
// The actual FFmpeg work happens asynchronously via processNext().
app.post('/transcode', async (req, res) => {
  const secret = req.headers['x-internal-secret'];
  if (!secret || secret !== process.env.INTERNAL_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }

  const { videoId } = req.body;
  if (!videoId) return res.status(400).json({ error: 'videoId is required' });

  enqueue(videoId);

  res.status(202).json({
    message: `Accepted. Video ${videoId} added to queue.`,
    queueDepth: jobQueue.length,
  });
});

// ─── Crash Recovery ───────────────────────────────────────────────────────────
// On startup, mark any videos stuck in 'processing' as 'failed'.
// They were in-flight or in-queue when the server last crashed.
async function recoverStuckVideos() {
  try {
    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
    const { data: stuckVideos, error } = await supabaseAdmin
      .from('videos')
      .select('id, title')
      .eq('status', 'processing')
      .lt('created_at', fiveMinutesAgo);

    if (error) { console.error('Crash recovery query failed:', error.message); return; }

    if (stuckVideos && stuckVideos.length > 0) {
      console.log(`🔄 Found ${stuckVideos.length} stuck video(s). Marking as failed.`);
      for (const video of stuckVideos) {
        await supabaseAdmin.from('videos').update({ status: 'failed' }).eq('id', video.id);
        console.log(`  ↳ Marked "${video.title}" (${video.id}) as failed`);
      }
    } else {
      console.log('✅ No stuck videos — clean startup.');
    }
  } catch (err) {
    console.error('Crash recovery error:', err.message);
  }
}

// ─── ffprobe helper ───────────────────────────────────────────────────────────
function getVideoDuration(inputUrl) {
  return new Promise((resolve) => {
    ffmpeg.ffprobe(inputUrl, (err, metadata) => {
      if (err || !metadata?.format?.duration) {
        console.warn('Could not extract duration:', err?.message || 'no duration in metadata');
        resolve(null);
      } else {
        resolve(Math.round(metadata.format.duration));
      }
    });
  });
}

// ─── Core Transcode Logic ─────────────────────────────────────────────────────
// Extracted into its own function so the queue worker can call it cleanly.
async function transcodeVideo(videoId) {
  const tempHlsDir = path.join('/tmp', `temp_hls_${videoId}`);
  fs.mkdirSync(tempHlsDir, { recursive: true });

  const localHlsPlaylistPath = path.join(tempHlsDir, 'playlist.m3u8');
  const localThumbnailPath   = path.join(tempHlsDir, 'thumbnail.png');
  const thumbnailFileName    = `${videoId}.png`;

  try {
    const { data: videoData, error: dbError } = await supabaseAdmin
      .from('videos')
      .select('raw_path')
      .eq('id', videoId)
      .single();

    if (dbError || !videoData) throw new Error(`Video not found for ID: ${videoId}`);

    // Stream raw video directly from Supabase — no local disk download
    const { data: signedData, error: signedError } = await supabaseAdmin.storage
      .from('raw_uploads')
      .createSignedUrl(videoData.raw_path, 3600);

    if (signedError || !signedData?.signedUrl) {
      throw new Error(`Failed to get signed URL: ${signedError?.message}`);
    }

    const inputStreamUrl = signedData.signedUrl;

    const durationSeconds = await getVideoDuration(inputStreamUrl);
    if (durationSeconds) console.log(`📏 Duration: ${durationSeconds}s`);

    // Thumbnail
    try {
      await new Promise((resolve) => {
        ffmpeg(inputStreamUrl)
          .screenshots({ timestamps: ['00:00:02'], filename: 'thumbnail.png', folder: tempHlsDir, size: '320x180' })
          .on('end', resolve)
          .on('error', (err) => { console.warn('Thumbnail failed:', err.message); resolve(); });
      });
    } catch (err) {
      console.warn('Thumbnail skipped:', err.message);
    }

    // HLS transcode
    await new Promise((resolve, reject) => {
      ffmpeg(inputStreamUrl)
        .outputOptions(['-c:v h264', '-hls_time 10', '-hls_list_size 0', '-f hls', '-vf scale=640:-2'])
        .output(localHlsPlaylistPath)
        .on('end', resolve)
        .on('error', (err) => { console.error('HLS transcode failed:', err.message); reject(err); })
        .run();
    });

    // Upload thumbnail
    if (fs.existsSync(localThumbnailPath)) {
      const thumbnailBuffer = fs.readFileSync(localThumbnailPath);
      const { error: thumbErr } = await supabaseAdmin.storage
        .from('thumbnails')
        .upload(thumbnailFileName, thumbnailBuffer, { contentType: 'image/png', upsert: true });
      if (thumbErr) console.warn('Thumbnail upload failed:', thumbErr.message);
    }

    // Upload HLS segments
    for (const file of fs.readdirSync(tempHlsDir)) {
      const fileBuffer = fs.readFileSync(path.join(tempHlsDir, file));
      const { error: uploadErr } = await supabaseAdmin.storage
        .from('hls')
        .upload(`${videoId}/${file}`, fileBuffer, {
          contentType: file.endsWith('.m3u8') ? 'application/vnd.apple.mpegurl' : 'video/MP2T',
          upsert: true,
        });
      if (uploadErr) console.warn(`HLS upload failed for ${file}:`, uploadErr.message);
    }

    const { error: updateErr } = await supabaseAdmin.from('videos').update({
      status: 'ready',
      hls_path: `${videoId}/playlist.m3u8`,
      thumbnail_path: fs.existsSync(localThumbnailPath) ? thumbnailFileName : null,
      ...(durationSeconds ? { duration_seconds: durationSeconds } : {}),
    }).eq('id', videoId);

    if (updateErr) {
      console.error(`❌ DB status update failed for ${videoId}:`, updateErr.message);
    } else {
      console.log(`✅ Video ${videoId} ready!`);

      // Feature 5: Delete the raw source file — HLS segments are now live and the
      // original is no longer needed. Raw files are typically far larger than HLS output.
      try {
        const { error: rawDeleteErr } = await supabaseAdmin.storage
          .from('raw_uploads')
          .remove([videoData.raw_path]);
        if (rawDeleteErr) {
          console.warn(`⚠️  Raw file cleanup failed for ${videoId}:`, rawDeleteErr.message);
        } else {
          console.log(`🧹 Raw file deleted: ${videoData.raw_path}`);
        }
      } catch (rawDeleteEx) {
        console.warn(`⚠️  Raw file cleanup exception for ${videoId}:`, rawDeleteEx.message);
      }
    }

  } catch (error) {
    console.error(`❌ Failed ${videoId}:`, error.message);
    await supabaseAdmin.from('videos').update({ status: 'failed' }).eq('id', videoId);

  } finally {
    fs.rmSync(tempHlsDir, { recursive: true, force: true });
    console.log(`🧹 Cleanup done for ${videoId}`);
  }
}

// ─── Start Server ─────────────────────────────────────────────────────────────
app.listen(port, () => {
  console.log(`🚀 Server running on port ${port}`);
  recoverStuckVideos();
});
