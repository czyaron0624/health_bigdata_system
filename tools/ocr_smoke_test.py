from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

from PIL import Image, ImageDraw

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from crawlers.ocr_utils import OCRProcessor


def main() -> int:
    os.environ.setdefault('OCR_BACKEND', 'rapidocr')

    image = Image.new('RGB', (320, 120), 'white')
    drawer = ImageDraw.Draw(image)
    drawer.text((20, 40), 'ABC123', fill='black')

    with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
        image.save(tmp.name, 'PNG')
        image_path = tmp.name

    try:
        processor = OCRProcessor()
        results = processor.recognize_local_image(image_path)

        print(f'backend={processor.backend}')
        print(f'result_count={len(results)}')
        for item in results:
            print(item)

        if processor.backend != 'rapidocr':
            raise SystemExit('expected rapidocr backend')
        if not results:
            raise SystemExit('expected at least one OCR result')

        return 0
    finally:
        if os.path.exists(image_path):
            os.remove(image_path)


if __name__ == '__main__':
    raise SystemExit(main())
