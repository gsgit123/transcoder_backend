const express = require('express');
const app = express();

const port=process.env.PORT||3001;

app.use(express.json());

app.get('/',(req,res)=>{
    res.send("Transcoding server is alive!");
})

app.post('/transcode',(req,res)=>{
    const {videoId}=req.body;
    if(!videoId){
        return res.status(400).send({error:"videoId is required"});
    }
    console.log(`Received request to transcode video with ID: ${videoId}`);

    res.status(202).send({ message: `Accepted. Processing video: ${videoId}` });



});

app.listen(port,()=>{
    console.log(`Server is running on port ${port}`);
})