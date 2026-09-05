FROM python:3.12.4

WORKDIR /code

# torch FIRST, and from the CPU-only index (ClickUp 86cbegg2r).
# ------------------------------------------------------------------
# `pip install torch` from PyPI pulls the CUDA build: ~2.5 GB of nvidia-*
# wheels this image can never use — no GPU is involved anywhere in this
# project, on any machine. The CPU index serves the same version without
# them, which is the difference between ~1.5 GB and ~5 GB of image.
#
# It is a separate layer, and the first one, on purpose: it is by far the
# largest and the slowest to install, and it changes only when this file
# does — so an edit to requirements.txt (or to the application) does not
# re-download it.
COPY ./requirements-torch.txt /code/requirements-torch.txt
RUN pip install --no-cache-dir \
    --index-url https://download.pytorch.org/whl/cpu \
    -r /code/requirements-torch.txt

# ...and the same file again as a CONSTRAINT below. Without it the second
# resolution backtracks on `torch>=2.2` (sentence-transformers' own
# requirement), decides to "upgrade" the perfectly good +cpu build, and
# fetches torch from PyPI with the whole CUDA stack behind it: measured
# 7.24 GB image instead of 2.63 GB. The constraint makes that impossible —
# a genuine conflict now fails the build loudly instead of silently
# installing 4.6 GB of unusable GPU libraries.
COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade \
    -c /code/requirements-torch.txt \
    -r /code/requirements.txt

# The model weights are a read-only volume (EMBEDDING_MODEL_PATH), never a
# download: this makes the "offline" promise structural rather than a
# habit. If anything ever asks the hub for a file, it fails loudly here
# instead of quietly fetching 2.3 GB on a production VM.
ENV HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1

COPY . /code

CMD ["fastapi", "run", "app/main.py", "--host", "0.0.0.0", "--port", "8000", "--no-proxy-headers"]
