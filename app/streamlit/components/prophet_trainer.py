from __future__ import annotations
import datetime
import pickle
import threading
from pathlib import Path
import pandas as pd
from prophet import Prophet


CACHE_DIR = Path(__file__).parent.parent / "cache" / "prophet_models"
CACHE_DIR.mkdir(parents=True, exist_ok=True)



# Per-process training state — key = f"{machine}_{metric}" -> values: "training" | "ready" | f"error: {msg}"
_training_status: dict[str, str] = {}


def model_path(machine: str, metric: str) -> Path:
    """
    Path ของ .pkl file สำหรับ (machine × metric)
    """
    return CACHE_DIR / f"{machine}_{metric}.pkl"



def model_status(machine: str, metric: str) -> dict:
    """
    Inspect cached model file

    Returns dict:
      exists: bool
      last_trained: datetime | None
      status_text: str (human-readable for UI)
    """

    # Format key
    key = f"{machine}_{metric}"

    # ถ้ากำลัง train อยู่ → return early
    if _training_status.get(key) == "training":
        return {
            "exists": False,
            "last_trained": None,
            "status_text": "Training in progress...",
        }

    # ตรวจ error state
    err = _training_status.get(key, "")
    if err.startswith("error:"):
        return {
            "exists": False,
            "last_trained": None,
            "status_text": err,
        }

    # Check File existed
    p = model_path(machine, metric)
    if not p.exists():
        return {
            "exists": False,
            "last_trained": None,
            "status_text": "Not trained yet — click Train",
        }

    mtime = datetime.datetime.fromtimestamp(p.stat().st_mtime)
    return {
        "exists": True,
        "last_trained": mtime,
        "status_text": f"Trained at {mtime.strftime('%Y-%m-%d %H:%M')}",
    }



def _train_in_thread(machine: str, metric: str, history_df: pd.DataFrame) -> None:
    """
    Internal: fit Prophet + save .pkl (รันใน daemon thread)
    """

    # Format Key
    key = f"{machine}_{metric}"

    # Check status == training
    _training_status[key] = "training"

    try:

        # Calling Model Prophet
        model = Prophet(
            daily_seasonality=True,
            weekly_seasonality=True,
            interval_width=0.95,
        )

        # Training Model
        model.fit(history_df)

        # Save to folder cache
        with open(model_path(machine, metric), "wb") as f:
            pickle.dump(model, f)
        
        # Set status
        _training_status[key] = "ready"

    except Exception as e:
        _training_status[key] = f"error: {e}"



def trigger_training(machine: str, metric: str, history_df: pd.DataFrame) -> None:
    """
    Start Prophet training ใน background thread
    history_df ต้องมี columns: ds (datetime), y (numeric)
    Raises:
        ValueError ถ้า history < 30 points
    """

    # Check Input Dataframe
    if history_df.empty or len(history_df) < 30:
        raise ValueError(
            f"Need ≥30 historical points to train; got {len(history_df)}"
        )

    # Thread Training
    thread = threading.Thread(
        target=_train_in_thread,
        args=(machine, metric, history_df.copy()),
        daemon=True,
    )

    # Running Thread
    thread.start()



def predict(machine: str, metric: str, hours: int) -> pd.DataFrame | None:
    """Load .pkl + forecast next N hours (ราย 15 นาที)

    Returns df with columns ds, yhat, yhat_lower, yhat_upper
    หรือ None ถ้าไม่มี model
    """

    # Set Path
    p = model_path(machine, metric)
    if not p.exists():
        return None

    # Check model
    with open(p, "rb") as f:
        model = pickle.load(f)

    # Create data format    
    future = model.make_future_dataframe(

        # 15-min steps → hours × 4 periods
        periods = hours * 4,
        freq = "15min",
        include_history = False,
    )

    # Inferences 
    forecast = model.predict(future)

    # Return format
    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]
