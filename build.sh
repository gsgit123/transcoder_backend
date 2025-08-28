#!/usr/bin/env bash
# exit on error
set -o errexit

# Install FFMPEG
apt-get update && apt-get install -y ffmpeg

# Install npm dependencies
npm install