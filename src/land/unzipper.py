import os
import zipfile
from utils.logger import log_event

def _is_within_directory(directory: str, target: str) -> bool:
    """
    Prevent zip-slip vulnerabilities by ensuring the extracted path
    stays inside the intended output directory.
    """
    abs_directory = os.path.abspath(directory)
    abs_target = os.path.abspath(target)
    return os.path.commonpath([abs_directory]) == os.path.commonpath([abs_directory, abs_target])


def unzip_file(zip_path: str, output_dir: str, event_id: str) -> str:
    """
    Extracts a large ZIP file into output_dir.
    Returns the output_dir.
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    log_event(
        f"Starting extraction of archive {os.path.basename(zip_path)}",
        event_id=event_id,
        event_type="extraction_start",
        file=zip_path,
        target_dir=output_dir,
    )

    with zipfile.ZipFile(zip_path, "r") as zf:
        members = zf.infolist()
        total_size = sum(m.file_size for m in members)  # total *uncompressed* size
        extracted_size = 0
        last_logged = 0

        for member in members:
            # Construct destination path
            extracted_path = os.path.join(output_dir, member.filename)

            if not _is_within_directory(output_dir, extracted_path):
                raise Exception(f"Unsafe zip entry detected: {member.filename}")

            # Extract file
            zf.extract(member, output_dir)

            # Update size
            extracted_size += member.file_size
            if total_size > 0:
                percent = (extracted_size / total_size) * 100
                if percent >= last_logged + 10:  # log every 10%
                    log_event(
                        f"Extraction progress: {percent:.1f}% complete",
                        archive=zip_path,
                        output_dir=output_dir,
                        event_id=event_id,
                        event_type="extraction_progress",
                        extracted_mb=extracted_size / (1024 * 1024),
                    )
                    last_logged = int(percent)

    log_event(
        f"Extraction complete for {os.path.basename(zip_path)}",
        event_id=event_id,
        event_type="extraction_complete",
        archive=zip_path,
        output_dir=output_dir,
    )

    return output_dir