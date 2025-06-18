import os
import io
import csv
import time
import pandas as pd
from pydub import AudioSegment
from google.cloud import speech_v1p1beta1 as speech
from google.oauth2 import service_account
from google.cloud import storage

# CONFIG
input_dir = r"C:\Users\Kritin.Sornmanee\OneDrive - Ipsos\Desktop\datasound\67-0323 Honda SSI & CSI (CSAT) 2024"
output_dir = os.path.join(input_dir, "transcripts")
os.makedirs(output_dir, exist_ok=True)
timestamp = int(time.time())

# ✅ Service account
client_file = 'sa_key2.json'
credentials = service_account.Credentials.from_service_account_file(client_file)
client = speech.SpeechClient(credentials=credentials)
storage_client = storage.Client(credentials=credentials)

# Audio conversion
def convert_to_wav(filepath):
    if not filepath.lower().endswith(".wav"):
        audio = AudioSegment.from_file(filepath)
        if filepath.lower().endswith(".mp4"):
            filepath_wav = filepath.replace(".mp4", ".wav")
        elif filepath.lower().endswith(".mp3"):
            filepath_wav = filepath.replace(".mp3", ".wav")
        else:
            filepath_wav = filepath + ".wav"
        audio.export(filepath_wav, format="wav")
        return filepath_wav
    return filepath

# GCS Upload
bucket_name = "mp4forqa"
def upload_to_gcs(local_filepath):
    blob_name = os.path.basename(local_filepath)
    blob = storage_client.bucket(bucket_name).blob(blob_name)
    blob.upload_from_filename(local_filepath)
    gcs_uri = f"gs://{bucket_name}/{blob_name}"
    return gcs_uri

# Transcription config
diarization_config = speech.SpeakerDiarizationConfig(
    enable_speaker_diarization=True,
    min_speaker_count=2,
    max_speaker_count=10,
)

def transcribe_audio_gcs(gcs_uri):
    audio = speech.RecognitionAudio(uri=gcs_uri)
    config = speech.RecognitionConfig(
        encoding=speech.RecognitionConfig.AudioEncoding.LINEAR16,
        language_code="th-TH",
        diarization_config=diarization_config,
        enable_word_time_offsets=True,
        model="latest_long"
    )
    operation = client.long_running_recognize(config=config, audio=audio)
    print("⏳ Waiting for result...")
    response = operation.result(timeout=300)
    return response

# ✅ Output Excel Writer
excel_path = os.path.join(output_dir, f"All_Transcripts_{timestamp}.xlsx")
excel_writer = pd.ExcelWriter(excel_path, engine='openpyxl')

# ✅ Error tracking
error_list = []
error_limit = 3000
error_count = 0

# ✅ Process files
for filename in os.listdir(input_dir):
    if filename.lower().endswith((".mp3", ".mp4", ".wav")):
        print(f"\n📂 Processing: {filename}")

        if error_count >= error_limit:
            print("🚨 Too many errors. Stopping...")
            break

        try:
            file_path = os.path.join(input_dir, filename)
            wav_path = convert_to_wav(file_path)
            gcs_uri = upload_to_gcs(wav_path)
            response = transcribe_audio_gcs(gcs_uri)

            result = response.results[-1] if response.results else None
            if not result or not result.alternatives:
                error_list.append({"Filename": filename, "Error Description": "No results or alternatives"})
                error_count += 1
                continue

            words_info = result.alternatives[0].words
            if not words_info:
                error_list.append({"Filename": filename, "Error Description": "No word info"})
                error_count += 1
                continue

            current_speaker = words_info[0].speaker_tag
            segment_words = []
            start_time = words_info[0].start_time.total_seconds()
            data = []

            for i, word_info in enumerate(words_info):
                if word_info.speaker_tag == current_speaker:
                    segment_words.append(word_info.word)
                else:
                    end_time = words_info[i - 1].end_time.total_seconds()
                    data.append({
                        "filename": filename,
                        "speaker": f"Speaker {current_speaker}",
                        "start_time": round(start_time, 2),
                        "end_time": round(end_time, 2),
                        "text": " ".join(segment_words)
                    })
                    current_speaker = word_info.speaker_tag
                    segment_words = [word_info.word]
                    start_time = word_info.start_time.total_seconds()

            if segment_words:
                end_time = words_info[-1].end_time.total_seconds()
                data.append({
                    "filename": filename,
                    "speaker": f"Speaker {current_speaker}",
                    "start_time": round(start_time, 2),
                    "end_time": round(end_time, 2),
                    "text": " ".join(segment_words)
                })

            df = pd.DataFrame(data)
            sheet_name = os.path.splitext(filename)[0][:31]
            df.to_excel(excel_writer, sheet_name=sheet_name, index=False)
            print(f"✅ Done: {filename}")

        except Exception as e:
            print(f"❌ Error: {filename}: {e}")
            error_list.append({"Filename": filename, "Error Description": f"Exception: {e}"})
            error_count += 1

# ✅ Save transcripts Excel
excel_writer.close()
print(f"\n📄 All transcripts saved to: {excel_path}")

# ✅ Save error report to Excel (if any)
if error_list:
    error_df = pd.DataFrame(error_list)
    error_excel_path = os.path.join(output_dir, f"Failed_Transcripts_{timestamp}.xlsx")
    with pd.ExcelWriter(error_excel_path, engine="openpyxl") as writer:
        error_df.to_excel(writer, sheet_name="Errors", index=False)
    print(f"📄 Error details saved to: {error_excel_path}")
else:
    print("✅ No errors encountered.")

