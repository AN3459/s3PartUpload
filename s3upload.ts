import { S3Client, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand } from "@aws-sdk/client-s3";
import { Readable } from "stream";
import * as fs from "fs";

// AWS 配置
const s3Client = new S3Client({
    region:'xxx',
    credentials: {
      accessKeyId:'xxxS',
      secretAccessKey: 'xxx',
    },
  });

// S3 存储桶的名称和对象键（文件名）
const bucketName = 'xxx';
const objectKey = xxx';

// 本地 OVA 文件的路径
const localFilePath = 'xx';

const uploadLargeFile = async () => {
    const fileStream = fs.createReadStream(localFilePath);
    const fileSize = fs.statSync(localFilePath).size;

    // 创建分段上传
    const createMultipartUploadCommand = new CreateMultipartUploadCommand({
      Bucket: bucketName,
      Key: objectKey,
    });
  
    const createMultipartUploadResponse = await s3Client.send(createMultipartUploadCommand);
    const uploadId = createMultipartUploadResponse.UploadId;

    if(uploadId === undefined){
        console.error('完成分段上传失败');
    }
  
    const partSize = 1024 * 1024 * 50; // 5MB 分段大小
    const numParts = Math.ceil(fileSize / partSize);

    const parts: { ETag: string | undefined; PartNumber: number; }[] = [];
  
    const uploadPartCommandList: UploadPartCommand[] = [];

    for (let partNumber = 1; partNumber <= numParts; partNumber++) {
      const start = (partNumber - 1) * partSize;
      const end = Math.min(start + partSize, fileSize);

      // 读取数据块
      // let chunk : Buffer
      // fileStream.on('data',()=>{
      //   const buffer = fileStream.read(end - start);
      //   chunk = buffer;
      // })
      // const buffer = fileStream.read(end - start);
      // const bytesRead = fileStream.read(buffer, 0, end - start, start);

      console.log('UploadPartCommand',{
        Bucket: bucketName,
        Key: objectKey,
        UploadId: uploadId,
        PartNumber: partNumber,
        Body: fileStream.read(end - start),
    })
      const uploadPartCommand = new UploadPartCommand({
          Bucket: bucketName,
          Key: objectKey,
          UploadId: uploadId,
          PartNumber: partNumber,
          Body: fileStream.read(end - start),
      });

      try {
          const uploadPartResponse = await s3Client.send(uploadPartCommand);
          console.log({
            ETag: uploadPartResponse.ETag,
            PartNumber: partNumber,
          });
          parts.push({
              ETag: uploadPartResponse.ETag,
              PartNumber: partNumber,
          });
          console.log(`Part ${partNumber} 已上传`);
      } catch (error) {
          console.error(`上传 Part ${partNumber} 失败:`, error);
      }
  }
    
    console.log('-----------------');
    console.log('read end');
    console.log(uploadPartCommandList);

    // await Promise.all(
    //   uploadPartCommandList.map(async (command : UploadPartCommand)=>{
    //     try{ 
    //       const uploadPartResponse = await s3Client.send(command);
    //       parts.push({
    //           ETag: uploadPartResponse.ETag,
    //           PartNumber: command.input.PartNumber!,
    //         });
    //         console.log('-----------------');
    //         console.log(`part${command.input.PartNumber!} send success`);
    //     }catch(e){
    //       console.log('-----------------');
    //       console.log('intenelError',e);
    //       console.log('ENDENDENDENDNENDENDNENNDENDNENDNENNDNE');
    //     }
    //   })
    // )

    console.log('-----------------');
    console.log('send end');
    console.log(parts);

    // 完成分段上传

    console.log('-----------------');
    console.log('complete upload');
    const completeMultipartUploadCommand = new CompleteMultipartUploadCommand({
      Bucket: bucketName,
      Key: objectKey,
      UploadId: uploadId,
      MultipartUpload: { Parts: parts },
    });
  
    console.log('-----------------');
    console.log('start send');
    const completeMultipartUploadResponse = await s3Client.send(completeMultipartUploadCommand);
    console.log('上传完成', completeMultipartUploadResponse.Location);
  }
  
  // 执行上传操作
  uploadLargeFile()
    .catch((err) => {
      console.error('上传失败', err);
    });
