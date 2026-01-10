import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { mkdir, writeFile, readFile, rm } from "node:fs/promises";
import { createWriteStream } from "node:fs";
import path from "node:path";
import archiver from "archiver";

const GRIST_SERVER = "https://grist.srirajabags.in";
const GRIST_API_KEY = process.env.GRIST_API_KEY; // Set this in Railway variables
const GRIST_DOC_ID = "8vRFY3UUf4spJroktByH4u";

const S3_BUCKET = process.env.S3_BUCKET || "";
const S3_REGION = process.env.AWS_REGION || "ap-south-1";

const s3Client = new S3Client({
    region: S3_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || "",
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "",
    },
});

const ALLOWED_ORIGINS = [
    "https://www.srirajabags.in",
    "https://grist.srirajabags.in",
];

async function fetchWithRetry(url: string, options: any, maxRetries = 10) {
    let lastError: any;
    for (let attempt = 0; attempt < maxRetries; attempt++) {
        try {
            const response = await fetch(url, options);
            if (response.ok) return response;

            // Clone the response to read the body without consuming the original
            const clonedResponse = response.clone();
            const text = await clonedResponse.text();

            if (text.includes("Too many backlogged requests") || text.includes("try again later?")) {
                const backoff = Math.pow(2, attempt) * 1000;
                console.warn(`[RETRY] Grist backlogged. Attempt ${attempt + 1}. Retrying in ${backoff}ms...`);
                await new Promise(resolve => setTimeout(resolve, backoff));
                continue;
            }

            return response; // Return the non-retriable error response
        } catch (error) {
            lastError = error;
            const backoff = Math.pow(2, attempt) * 1000;
            console.error(`[RETRY] Fetch failed. Attempt ${attempt + 1}. Retrying in ${backoff}ms...`, error);
            await new Promise(resolve => setTimeout(resolve, backoff));
        }
    }
    throw lastError || new Error("Max retries reached");
}

Bun.serve({
    port: process.env.PORT || 3000,
    async fetch(req) {
        const url = new URL(req.url);
        const origin = req.headers.get("Origin");

        // 1. Handle CORS Preflight
        if (req.method === "OPTIONS") {
            return new Response(null, {
                headers: {
                    "Access-Control-Allow-Origin":
                        origin && ALLOWED_ORIGINS.includes(origin)
                            ? origin
                            : ALLOWED_ORIGINS[0],
                    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
                    "Access-Control-Allow-Headers": "Authorization, Content-Type",
                },
            });
        }

        // 2. Routing
        const pathParts = url.pathname.split("/"); // ["", "download/metadata", "docId", "attId"]

        if (pathParts.length === 4 && (pathParts[1] === "download" || pathParts[1] === "metadata")) {
            const [_, mode, docId, attId] = pathParts;
            const isMetadata = mode === "metadata";
            const gristUrl = isMetadata
                ? `${GRIST_SERVER}/api/docs/${docId}/attachments/${attId}`
                : `${GRIST_SERVER}/api/docs/${docId}/attachments/${attId}/download`;

            try {
                const response = await fetchWithRetry(gristUrl, {
                    headers: { Authorization: `Bearer ${GRIST_API_KEY}` },
                });

                if (!response.ok)
                    return new Response(`Grist Error: ${response.statusText}`, { status: response.status });

                // Stream the response back to the browser
                return new Response(response.body, {
                    headers: {
                        "Access-Control-Allow-Origin":
                            origin && ALLOWED_ORIGINS.includes(origin)
                                ? origin
                                : ALLOWED_ORIGINS[0],
                        "Access-Control-Expose-Headers": "Content-Disposition, Content-Type",
                        "Content-Type":
                            response.headers.get("Content-Type") ||
                            "application/octet-stream",
                        "Content-Disposition":
                            response.headers.get("Content-Disposition") || "attachment",
                    },
                });
            } catch (e) {
                return new Response("Proxy Error", { status: 500 });
            }
        }

        // 3. Plate Orders Batch Link
        if (req.method === "POST" && url.pathname === "/plate-orders-batch-link") {
            const body = await req.json();
            const platesOrderId = body.Plates_Order_ID;

            if (!platesOrderId) {
                return new Response("Missing Plates_Order_ID", { status: 400 });
            }

            const encoder = new TextEncoder();
            let isClosed = false;
            let heartbeatInterval: any;

            const stream = new ReadableStream({
                async start(controller) {
                    const send = (data: any) => {
                        if (isClosed) return;
                        try {
                            controller.enqueue(encoder.encode(`data: ${JSON.stringify(data)}\n\n`));
                        } catch (e) {
                            console.error("[DEBUG] Failed to enqueue data, controller likely closed", e);
                            isClosed = true;
                        }
                    };

                    // Keep-alive heartbeat to prevent Railway proxy timeout
                    heartbeatInterval = setInterval(() => {
                        if (isClosed) return;
                        try {
                            controller.enqueue(encoder.encode(": keep-alive\n\n"));
                        } catch (e) {
                            isClosed = true;
                            if (heartbeatInterval) clearInterval(heartbeatInterval);
                        }
                    }, 5000);

                    try {
                        console.log(`[DEBUG] Starting workflow for batch: ${platesOrderId}`);

                        // 1. Query Plates_Order_Batches to get internal ID
                        console.log(`[DEBUG] Querying Plates_Order_Batches for ID: ${platesOrderId}...`);
                        send({ stage: "Locating Batch Record", status: "started" });

                        const batchFilter = JSON.stringify({ Plates_Order_ID: [platesOrderId] });
                        const batchUrl = `${GRIST_SERVER}/api/docs/${GRIST_DOC_ID}/tables/Plates_Order_Batches/records?filter=${encodeURIComponent(batchFilter)}`;
                        const batchResponse = await fetchWithRetry(batchUrl, {
                            headers: { Authorization: `Bearer ${GRIST_API_KEY}` },
                        });

                        if (!batchResponse.ok) {
                            throw new Error(`Grist Batch Query Error: ${batchResponse.statusText}`);
                        }

                        const { records: batchRecords } = await batchResponse.json();
                        if (batchRecords.length === 0) {
                            throw new Error(`Batch record not found for: ${platesOrderId}`);
                        }

                        const batchRecord = batchRecords[0];
                        const internalBatchId = batchRecord.id;
                        const existingLink = batchRecord.fields.TIFF_Files_Download_Link;

                        if (existingLink && existingLink.trim() !== "") {
                            console.log(`[DEBUG] Existing download link found: ${existingLink}. Skipping processing.`);
                            send({ stage: "Workflow Completed", status: "success", url: existingLink, message: "Returned existing link from Grist." });
                            return; // Exit early
                        }

                        console.log(`[DEBUG] Found internal batch ID: ${internalBatchId}`);
                        send({ stage: "Locating Batch Record", status: "completed" });

                        // 2. Query Plate_Orders
                        console.log(`[DEBUG] Querying Plate_Orders for batch ID: ${internalBatchId}...`);
                        send({ stage: "Searching Plate Order Records", status: "started" });
                        const plateFilter = JSON.stringify({ Plate_Orders_Batch: [internalBatchId] });
                        const plateUrl = `${GRIST_SERVER}/api/docs/${GRIST_DOC_ID}/tables/Plate_Orders/records?filter=${encodeURIComponent(plateFilter)}`;
                        const gristResponse = await fetchWithRetry(plateUrl, {
                            headers: { Authorization: `Bearer ${GRIST_API_KEY}` },
                        });

                        if (!gristResponse.ok) {
                            throw new Error(`Grist Plate Query Error: ${gristResponse.statusText}`);
                        }

                        const { records } = await gristResponse.json();
                        console.log(`[DEBUG] Grist returned ${records.length} records`);
                        const plates = records.flatMap((r: any) => {
                            const tiffFile = r.fields.TIFF_File;
                            // Grist attachment column format: ["L", attId1, attId2, ...]
                            if (Array.isArray(tiffFile) && tiffFile[0] === "L") {
                                return tiffFile.slice(1).map(attId => ({ attId, ...r.fields }));
                            }
                            return [];
                        });

                        console.log(`[DEBUG] Total plates found: ${plates.length}`);
                        send({ stage: "Searching Plate Order Records", status: "completed", count: plates.length });

                        if (plates.length === 0) {
                            throw new Error("No plates found for this Order ID.");
                        }

                        // 2. Download
                        const tmpDir = path.join("/tmp", platesOrderId);
                        console.log(`[DEBUG] Creating temporary directory: ${tmpDir}`);
                        await mkdir(tmpDir, { recursive: true });

                        console.log(`[DEBUG] Starting downloads for ${plates.length} plates (limit: 50 at a time)...`);
                        send({ stage: "Downloading Attachments", status: "started", total: plates.length });
                        let downloadedCount = 0;

                        const downloadQueue = [...plates];
                        const CONCURRENCY_LIMIT = 20;

                        const workers = Array(Math.min(CONCURRENCY_LIMIT, plates.length)).fill(null).map(async () => {
                            while (downloadQueue.length > 0) {
                                const plate = downloadQueue.shift();
                                if (!plate) break;

                                const downloadUrl = `${GRIST_SERVER}/api/docs/${GRIST_DOC_ID}/attachments/${plate.attId}/download`;
                                const resp = await fetchWithRetry(downloadUrl, {
                                    headers: { Authorization: `Bearer ${GRIST_API_KEY}` },
                                });
                                if (!resp.ok) throw new Error(`Failed to download attachment ${plate.attId}`);

                                const cd = resp.headers.get("Content-Disposition");
                                let filename = `plate_${plate.attId}.tiff`;
                                if (cd && cd.includes("filename=")) {
                                    filename = cd.split("filename=")[1].split(";")[0].replace(/"/g, "").trim();
                                }

                                const orderType = plate.Order_Type || "Unknown_Type";
                                const groupFolder = plate.Group_Folder || "Unknown_Group";

                                // Organize as: {Plates_Order_ID}/{Order_Type}/{Group_Folder}/{filename}
                                const fileSubDir = path.join(tmpDir, platesOrderId, orderType, groupFolder);
                                await mkdir(fileSubDir, { recursive: true });

                                const buffer = await resp.arrayBuffer();
                                const filePath = path.join(fileSubDir, filename);
                                await writeFile(filePath, new Uint8Array(buffer));
                                console.log(`[DEBUG] Saved: ${filename} to ${filePath}`);

                                downloadedCount++;
                                send({
                                    stage: "Downloading Attachments",
                                    status: "progress",
                                    progress: Math.round((downloadedCount / plates.length) * 100),
                                    current: filename
                                });
                            }
                        });

                        await Promise.all(workers);
                        send({ stage: "Downloading Attachments", status: "completed" });
                        console.log(`[DEBUG] All downloads completed`);

                        // 3. Zip
                        console.log(`[DEBUG] Starting zipping operation...`);
                        send({ stage: "Zipping Folder", status: "started" });
                        const zipPath = `${tmpDir}.zip`;

                        await new Promise((resolve, reject) => {
                            const output = createWriteStream(zipPath);
                            const archive = archiver("zip", { zlib: { level: 1 } });

                            output.on("close", resolve);
                            archive.on("error", reject);

                            archive.pipe(output);
                            archive.directory(tmpDir, false);
                            archive.finalize();
                        });

                        console.log(`[DEBUG] ZIP file created at: ${zipPath}`);
                        send({ stage: "Zipping Folder", status: "completed" });

                        // 4. S3 Upload
                        console.log(`[DEBUG] Starting S3 upload to bucket: ${S3_BUCKET}...`);
                        send({ stage: "Uploading to S3", status: "started" });
                        const zipContent = await readFile(zipPath);
                        const s3Key = `${process.env.S3_PREFIX || "tiff-files"}/${platesOrderId}.zip`;

                        await s3Client.send(new PutObjectCommand({
                            Bucket: S3_BUCKET,
                            Key: s3Key,
                            Body: zipContent,
                            ContentType: "application/zip",
                        }));
                        console.log(`[DEBUG] Upload successful for key: ${s3Key}`);

                        const presignedUrl = await getSignedUrl(s3Client, new GetObjectCommand({
                            Bucket: S3_BUCKET,
                            Key: s3Key,
                        }), { expiresIn: 604800 }); // 7 days

                        console.log(`[DEBUG] Generated presigned URL: ${presignedUrl}`);
                        send({ stage: "Uploading to S3", status: "completed", url: presignedUrl });

                        // 5. Update Grist with download link
                        console.log(`[DEBUG] Updating Grist record ${internalBatchId} with download link...`);
                        send({ stage: "Saving Link to Grist", status: "started" });

                        const updateUrl = `${GRIST_SERVER}/api/docs/${GRIST_DOC_ID}/tables/Plates_Order_Batches/records`;
                        const updateResponse = await fetchWithRetry(updateUrl, {
                            method: "PATCH",
                            headers: {
                                Authorization: `Bearer ${GRIST_API_KEY}`,
                                "Content-Type": "application/json"
                            },
                            body: JSON.stringify({
                                records: [{
                                    id: internalBatchId,
                                    fields: {
                                        TIFF_Files_Download_Link: presignedUrl
                                    }
                                }]
                            })
                        });

                        if (!updateResponse.ok) {
                            throw new Error(`Failed to update Grist with download link: ${updateResponse.statusText}`);
                        }
                        console.log(`[DEBUG] Grist record updated successfully`);
                        send({ stage: "Saving Link to Grist", status: "completed" });

                        // Cleanup ephemeral storage
                        console.log(`[DEBUG] Cleaning up temporary files...`);
                        await rm(tmpDir, { recursive: true, force: true });
                        await rm(zipPath, { force: true });
                        console.log(`[DEBUG] Cleanup completed`);

                        send({ stage: "Workflow Completed", status: "success", url: presignedUrl });
                        console.log(`[DEBUG] Workflow finished successfully for ${platesOrderId}`);
                    } catch (error: any) {
                        console.error(error);
                        send({ stage: "Error", status: "failed", message: error.message });
                    } finally {
                        if (heartbeatInterval) clearInterval(heartbeatInterval);
                        if (!isClosed) {
                            try {
                                controller.close();
                            } catch (e) {
                                // Already closed
                            }
                        }
                    }
                },
                cancel() {
                    console.log("[DEBUG] Client disconnected, cancelling stream");
                    isClosed = true;
                    if (heartbeatInterval) clearInterval(heartbeatInterval);
                },
            });

            return new Response(stream, {
                headers: {
                    "Access-Control-Allow-Origin":
                        origin && ALLOWED_ORIGINS.includes(origin)
                            ? origin
                            : ALLOWED_ORIGINS[0],
                    "Content-Type": "text/event-stream",
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                },
            });
        }

        return new Response("Not Found", { status: 404 });
    },
});

console.log("Bun Proxy running...");
