from fastapi import FastAPI, UploadFile, File
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
import tempfile, os, shutil, io, base64
from processor import process_files

app = FastAPI()

@app.post("/process")
async def process_reports(cortex_file: UploadFile = File(...), adp_file: UploadFile = File(...)):
    with tempfile.TemporaryDirectory() as tmp:
        cortex_path = os.path.join(tmp, cortex_file.filename)
        adp_path = os.path.join(tmp, adp_file.filename)

        with open(cortex_path, "wb") as f:
            shutil.copyfileobj(cortex_file.file, f)
        with open(adp_path, "wb") as f:
            shutil.copyfileobj(adp_file.file, f)

        result = process_files(cortex_path, adp_path, tmp)
        excel_path = result["excel_file"]
        summary = result["summary"]

        # Stream file and delete afterward
        def file_iterator(path):
            with open(path, "rb") as f:
                data = f.read()
            return io.BytesIO(data)

        # Encode summary safely for HTTP headers
        encoded_summary = base64.b64encode(summary.encode("utf-8")).decode("utf-8")

        task = BackgroundTask(lambda: os.remove(excel_path) if os.path.exists(excel_path) else None)
        filename = os.path.basename(excel_path)
        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Summary-Encoded": encoded_summary
        }

        return StreamingResponse(
            file_iterator(excel_path),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
            background=task
        )
