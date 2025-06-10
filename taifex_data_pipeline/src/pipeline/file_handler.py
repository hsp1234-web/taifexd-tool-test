# src/pipeline/file_handler.py
# -*- coding: utf-8 -*-

# Intelligent File Handler: Identifies file types and handles archives.

import os
import shutil
import magic # python-magic library
import patoolib # patool library

try:
    from ..utils.logger import get_logger
    from ..config import settings
except ImportError:
    import sys
    current_script_path = os.path.abspath(__file__)
    project_root_for_direct_run = os.path.dirname(os.path.dirname(os.path.dirname(current_script_path)))
    if project_root_for_direct_run not in sys.path:
        sys.path.insert(0, project_root_for_direct_run)
    from src.utils.logger import get_logger # type: ignore
    from src.config import settings # type: ignore

logger = get_logger()

def get_file_mime_type(file_path):
    if not os.path.exists(file_path):
        logger.error(f"File [{file_path}] does not exist. Cannot detect MIME type.")
        return None
    if not os.access(file_path, os.R_OK):
        logger.error(f"File [{file_path}] is not readable. Cannot detect MIME type.")
        return None
    try:
        mime_type = magic.from_file(file_path, mime=True)
        logger.info(f"MIME type for file [{os.path.basename(file_path)}]: {mime_type}")
        return mime_type
    except Exception as e:
        logger.error(f"Error detecting MIME for [{os.path.basename(file_path)}]: {e}", exc_info=True)
        return None

def extract_archive(archive_path, output_directory):
    extracted_files_list = []
    try:
        base_archive_name = os.path.basename(archive_path)
        logger.info(f"Extracting [{base_archive_name}] to [{output_directory}]")
        if os.path.exists(output_directory):
            shutil.rmtree(output_directory)
        os.makedirs(output_directory, exist_ok=True)
        patoolib.extract_archive(archive_path, outdir=output_directory, verbosity=-1)
        logger.info(f"Extraction command for [{base_archive_name}] completed.")
        for root, _, files in os.walk(output_directory):
            for file_name in files:
                if not file_name.startswith("."): # Ignore hidden files
                    full_path = os.path.join(root, file_name)
                    extracted_files_list.append(os.path.abspath(full_path))
        if not extracted_files_list:
            logger.warning(f"No files found after extracting [{base_archive_name}] from [{output_directory}].")
        return extracted_files_list
    except Exception as e:
        logger.error(f"Extraction failed for [{os.path.basename(archive_path)}]: {e}", exc_info=True)
        if os.path.exists(output_directory): shutil.rmtree(output_directory, ignore_errors=True)
        return []

def handle_uploaded_file(source_file_path, temp_processing_dir_base):
    base_filename = os.path.basename(source_file_path)
    filename_no_ext, _ = os.path.splitext(base_filename)
    logger.info(f"Handling file: [{base_filename}], base temp dir: [{temp_processing_dir_base}]")
    if not os.path.exists(source_file_path) or not os.access(source_file_path, os.R_OK):
        logger.error(f"Source file [{source_file_path}] not accessible.")
        return "error/file-not-accessible", []
    os.makedirs(temp_processing_dir_base, exist_ok=True)
    mime_type = get_file_mime_type(source_file_path)
    if mime_type is None:
        logger.warning(f"MIME detection failed for [{base_filename}].")
        return "error/mime-detection-failed", []
    processed_file_paths = []
    if mime_type in settings.SUPPORTED_MIME_TYPES:
        file_category = settings.SUPPORTED_MIME_TYPES[mime_type]
        logger.info(f"File [{base_filename}] is category: {file_category} (MIME: {mime_type})")
        if file_category in ["zip", "rar", "7z", "gz", "tar", "bz2"]:
            specific_extraction_dir = os.path.join(temp_processing_dir_base, filename_no_ext)
            logger.info(f"Archive [{base_filename}] to be extracted to: [{specific_extraction_dir}]")
            processed_file_paths = extract_archive(source_file_path, specific_extraction_dir)
            if not processed_file_paths:
                logger.warning(f"Archive [{base_filename}] (MIME: {mime_type}) yielded no files.")
                return mime_type, []
        elif file_category in ["text", "csv"]:
            try:
                destination_path = os.path.join(temp_processing_dir_base, base_filename)
                if os.path.abspath(source_file_path) != os.path.abspath(destination_path):
                    shutil.copy2(source_file_path, destination_path)
                logger.info(f"File [{base_filename}] (type: {mime_type}) copied/kept at [{destination_path}]")
                processed_file_paths.append(os.path.abspath(destination_path))
            except Exception as e:
                logger.error(f"Copying file [{base_filename}] to [{temp_processing_dir_base}] failed: {e}", exc_info=True)
                return mime_type, []
        else:
            logger.error(f"MIME type {mime_type} ({base_filename}) in supported list but no processing logic.")
            return mime_type, []
    else:
        logger.warning(f"MIME type ({mime_type}) of file [{base_filename}] is not supported.")
        return mime_type, []
    logger.info(f"File [{base_filename}] processing finished. Produced {len(processed_file_paths)} file(s).")
    return mime_type, processed_file_paths
