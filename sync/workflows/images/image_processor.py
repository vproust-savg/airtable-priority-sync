"""
Image processing for the Airtable → Priority image sync.

Replicates the Photoshop Compress_v1.2.jsx logic using Pillow:
  - Convert to RGB (flatten transparency with white background)
  - Convert to sRGB color profile
  - Smart JPG compression to stay under MAX_SIZE_KB
  - Progressive downscale (75%) if lowest quality still exceeds limit
"""

from __future__ import annotations

import io
import logging

from PIL import Image, ImageCms

logger = logging.getLogger(__name__)

# Default max file size in KB
DEFAULT_MAX_SIZE_KB = 150


def process_image(
    image_bytes: bytes,
    max_size_kb: int = DEFAULT_MAX_SIZE_KB,
) -> bytes:
    """
    Process raw image bytes into a compressed JPG suitable for Priority.

    Steps:
      1. Open image
      2. Convert RGBA/P → RGB with white background
      3. Convert to sRGB color profile (if ICC profile present)
      4. Smart compress: binary search on JPEG quality to stay under max_size_kb
      5. Progressive downscale (75%) if quality 1 still exceeds limit

    Returns:
        Compressed JPG bytes.

    Raises:
        ValueError: If the image cannot be processed.
    """
    max_size_bytes = max_size_kb * 1024

    img = Image.open(io.BytesIO(image_bytes))

    # Step 1: Convert to RGB (handle transparency)
    img = _ensure_rgb(img)

    # Step 2: Convert to sRGB
    img = _convert_to_srgb(img)

    # Step 3: Smart compress
    return _smart_compress(img, max_size_bytes)


def _ensure_rgb(img: Image.Image) -> Image.Image:
    """Convert RGBA/P/LA images to RGB with white background."""
    if img.mode in ("RGBA", "LA", "PA"):
        background = Image.new("RGB", img.size, (255, 255, 255))
        # Use the alpha channel as mask
        if img.mode == "LA":
            img = img.convert("RGBA")
        elif img.mode == "PA":
            img = img.convert("RGBA")
        background.paste(img, mask=img.split()[-1])
        return background
    elif img.mode == "P":
        # Palette mode — may have transparency
        if "transparency" in img.info:
            img = img.convert("RGBA")
            return _ensure_rgb(img)
        return img.convert("RGB")
    elif img.mode != "RGB":
        return img.convert("RGB")
    return img


def _convert_to_srgb(img: Image.Image) -> Image.Image:
    """Convert to sRGB if the image has an embedded ICC profile."""
    icc_profile = img.info.get("icc_profile")
    if not icc_profile:
        return img

    try:
        src_profile = ImageCms.ImageCmsProfile(io.BytesIO(icc_profile))
        srgb_profile = ImageCms.createProfile("sRGB")
        img = ImageCms.profileToProfile(
            img, src_profile, srgb_profile,
            outputMode="RGB",
        )
    except Exception as e:
        logger.debug("sRGB conversion skipped: %s", e)

    return img


def _save_jpg(img: Image.Image, quality: int) -> bytes:
    """Save image as JPG to bytes buffer at given quality."""
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=quality, optimize=True)
    return buf.getvalue()


def _smart_compress(img: Image.Image, max_size_bytes: int) -> bytes:
    """
    Binary search on JPEG quality to find the highest quality under max_size_bytes.
    If quality 1 still exceeds the limit, progressively downscale by 75%.
    """
    max_downscale_attempts = 5

    for attempt in range(max_downscale_attempts + 1):
        # Try quality 85 first (good default)
        data = _save_jpg(img, 85)
        if len(data) <= max_size_bytes:
            # 85 works — try going higher
            lo, hi, best = 85, 95, data
            while lo <= hi:
                mid = (lo + hi) // 2
                test = _save_jpg(img, mid)
                if len(test) <= max_size_bytes:
                    best = test
                    lo = mid + 1
                else:
                    hi = mid - 1
            return best

        # Try quality 1 (minimum)
        data_min = _save_jpg(img, 1)
        if len(data_min) > max_size_bytes:
            # Even minimum quality too large — downscale
            w, h = img.size
            new_w = int(w * 0.75)
            new_h = int(h * 0.75)
            if new_w < 10 or new_h < 10:
                logger.warning("Image too large even at minimum size, returning quality 1")
                return data_min
            img = img.resize((new_w, new_h), Image.LANCZOS)
            logger.debug("Downscaled to %dx%d (attempt %d)", new_w, new_h, attempt + 1)
            continue

        # Binary search between 1 and 85
        lo, hi, best = 1, 84, data_min
        while lo <= hi:
            mid = (lo + hi) // 2
            test = _save_jpg(img, mid)
            if len(test) <= max_size_bytes:
                best = test
                lo = mid + 1
            else:
                hi = mid - 1

        return best

    # Fallback: return whatever we have at quality 1
    return _save_jpg(img, 1)
