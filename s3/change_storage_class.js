import { S3Client, ListObjectsV2Command, CopyObjectCommand } from "@aws-sdk/client-s3";
import dotenv from "dotenv";
dotenv.config();

const s3Client = new S3Client({
    region: process.env.REGION,
    credentials: {
        accessKeyId: process.env.ACCESS_KEY_ID,
        secretAccessKey: process.env.SECRET_ACCESS_KEY,
    },
});

const objects = async () => {
    const ListObjectsResponse = await s3Client.send(
        new ListObjectsV2Command({
            Bucket: process.env.BUCKET_NAME,
            Prefix: process.env.PREFIX,
        })
    );

    const listObjects = ListObjectsResponse.Contents;
    if (listObjects && listObjects.length > 0) {
        PrintObjectsFunction(listObjects);
    } else {
        console.log("No objects found in the specified bucket and prefix.");
    }
    return listObjects;
}

const PrintObjectsFunction = (listObjects) => {
    console.table(listObjects.map((item) => ({
        Key: item.Key,
        Size: item.Size,
        LastModified: item.LastModified,
        StorageClass: item.StorageClass,
    })));
}

const targetStorageClass = process.env.TARGET_STORAGE_CLASS;
for (const item of await objects()) {

    if (item.StorageClass != targetStorageClass) {
        await s3Client.send(
            new CopyObjectCommand({
                Bucket: process.env.BUCKET_NAME,
                Key: item.Key,
                StorageClass: targetStorageClass,
                CopySource: `${process.env.BUCKET_NAME}/${item.Key}`,
                MetadataDirective: "COPY",
                MetadataDirective: "COPY"
            })
        );
    } else {
        console.log(`Skipping ${item.Key} as it is already in ${targetStorageClass} storage class.`);
    }
}

console.log("\nAfter changing storage class, the objects are:");
await objects();