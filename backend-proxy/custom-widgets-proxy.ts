const GRIST_SERVER = "https://grist.srirajabags.in";
const GRIST_API_KEY = process.env.GRIST_API_KEY; // Set this in Railway variables
const ALLOWED_ORIGINS = [
    "https://www.srirajabags.in",
    "https://grist.srirajabags.in",
];

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
                    "Access-Control-Allow-Methods": "GET, OPTIONS",
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
                const response = await fetch(gristUrl, {
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

        return new Response("Not Found", { status: 404 });
    },
});

console.log("Bun Proxy running...");
