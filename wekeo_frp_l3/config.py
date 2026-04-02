from wekeo_frp_l3.hygeos_core import env
output_dir = env.getdir("OUTPUT_DIR")
frp_download_dir = env.getdir("DIR_ANCILLARY") / "SLSTR_FRP"
frp_download_dir.mkdir(parents=False, exist_ok=True)

if not frp_download_dir.exists():
    raise FileNotFoundError(f"FRP download directory {frp_download_dir} does not exist. Please create it or check your environment configuration.")

if not output_dir.exists():
    raise FileNotFoundError(f"Output directory {output_dir} does not exist. Please create it or check your environment configuration.")

failed_fpr_dir = env.getdir("DIR_ANCILLARY") / "SLSTR_FRP_FAILED"
failed_fpr_dir.mkdir(parents=False, exist_ok=True)

log_event_dir = output_dir / "log_event"
log_event_dir.mkdir(parents=False, exist_ok=True)


gridded_log_event_dir = output_dir / "gridded_log_event"
gridded_log_event_dir.mkdir(parents=False, exist_ok=True)