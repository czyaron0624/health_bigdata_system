"""
OCR工具模块
功能：封装PaddleOCR，提供图片文字识别能力
"""

import os
import inspect
import requests
import tempfile
from PIL import Image
from io import BytesIO
import logging
from typing import Any

logger = logging.getLogger(__name__)

class OCRProcessor:
    def __init__(self, use_gpu=False):
        """
        初始化OCR处理器
        :param use_gpu: 是否使用GPU加速（需要CUDA环境）
        """
        self.ocr = None
        self.rapidocr = None
        self.backend = None
        self.use_gpu = use_gpu
        os.environ.setdefault("PADDLE_PDX_DISABLE_MODEL_SOURCE_CHECK", "True")
        os.environ.setdefault("FLAGS_use_mkldnn", "0")
        os.environ.setdefault("FLAGS_enable_pir_api", "0")
        self.force_rapidocr = os.environ.get("OCR_BACKEND", "rapidocr").strip().lower() != "paddleocr"
        self._init_ocr()
    
    def _init_ocr(self):
        """延迟初始化OCR引擎"""
        if self.force_rapidocr:
            self._init_rapidocr()
            return

        try:
            from paddleocr import PaddleOCR

            init_kwargs = {
                'lang': 'ch',
            }

            signature = inspect.signature(PaddleOCR)
            if 'device' in signature.parameters and self.use_gpu:
                init_kwargs['device'] = 'gpu'

            self.ocr = PaddleOCR(**init_kwargs)
            self.backend = 'paddleocr'
            logger.info("✅ PaddleOCR 初始化成功")
        except Exception as paddleocr_error:
            logger.warning(f"⚠️ PaddleOCR 初始化失败，尝试 RapidOCR 兜底: {paddleocr_error}")
            self._init_rapidocr()

    def _init_rapidocr(self):
        try:
            from rapidocr_onnxruntime import RapidOCR

            self.rapidocr = RapidOCR()
            self.ocr = self.rapidocr
            self.backend = 'rapidocr'
            logger.info("✅ RapidOCR 初始化成功")
        except ImportError:
            logger.error("❌ 请安装 paddleocr 或 rapidocr-onnxruntime")
            raise
        except Exception as e:
            logger.error(f"❌ OCR初始化失败: {e}")
            raise

    def _run_ocr(self, image_path):
        """执行OCR识别并兼容不同后端返回格式"""
        if self.backend == 'rapidocr':
            rapidocr: Any = self.ocr
            result = rapidocr(image_path)
            if isinstance(result, tuple):
                return result[0] or []
            return result or []

        if hasattr(self.ocr, 'predict'):
            paddleocr: Any = self.ocr
            return paddleocr.predict(image_path)

        paddleocr: Any = self.ocr
        return paddleocr.ocr(image_path)

    def _should_fallback_to_rapidocr(self, error: Exception) -> bool:
        message = str(error)
        return any(
            token in message
            for token in (
                'ConvertPirAttribute2RuntimeAttribute',
                'onednn',
                'oneDNN',
                'pir::',
                'NotImplementedError',
            )
        )

    def _run_with_fallback(self, image_path):
        try:
            result = self._run_ocr(image_path)
            return self._normalize_result(result)
        except Exception as error:
            if self.backend != 'rapidocr' and self._should_fallback_to_rapidocr(error):
                logger.warning(f"⚠️ PaddleOCR 推理失败，切换 RapidOCR 重试: {error}")
                self._init_rapidocr()
                result = self._run_ocr(image_path)
                return self._normalize_result(result)
            raise

    def _normalize_result(self, result):
        """统一不同OCR后端的返回格式"""
        texts = []

        if not result:
            return texts

        # RapidOCR 格式: [[box, text, conf], ...]
        if isinstance(result, list) and result:
            first_line = result[0]
            if (
                isinstance(first_line, (list, tuple))
                and len(first_line) >= 3
                and isinstance(first_line[1], str)
            ):
                for line in result:
                    if not (isinstance(line, (list, tuple)) and len(line) >= 3 and isinstance(line[1], str)):
                        continue
                    texts.append({
                        'text': line[1],
                        'confidence': line[2],
                        'box': line[0]
                    })
                if texts:
                    return texts

        # PaddleOCR 旧格式: [[box, (text, conf)], ...]
        if isinstance(result, list) and result and isinstance(result[0], list):
            first_item = result[0]
            if first_item and isinstance(first_item[0], (list, tuple)) and len(first_item[0]) >= 2:
                for line in first_item:
                    if not (
                        isinstance(line, (list, tuple))
                        and len(line) >= 2
                        and isinstance(line[1], (list, tuple))
                        and len(line[1]) >= 2
                    ):
                        continue
                    texts.append({
                        'text': line[1][0],
                        'confidence': line[1][1],
                        'box': line[0]
                    })
                if texts:
                    return texts

        # PaddleOCR 新格式对象
        first_item = result[0] if isinstance(result, list) and result else result
        if hasattr(first_item, 'res'):
            for line in getattr(first_item, 'res', []):
                if isinstance(line, dict):
                    texts.append({
                        'text': line.get('text', ''),
                        'confidence': line.get('confidence', 0),
                        'box': line.get('box', []),
                    })

        return texts
    
    def recognize_from_url(self, image_url, headers=None):
        """
        从图片URL识别文字
        :param image_url: 图片链接
        :param headers: 请求头
        :return: 识别出的文字列表
        """
        try:
            # 下载图片
            if headers is None:
                headers = {'User-Agent': 'Mozilla/5.0'}
            
            response = requests.get(image_url, headers=headers, timeout=15, verify=False)
            response.raise_for_status()
            
            # 转换为PIL Image
            image = Image.open(BytesIO(response.content))
            
            return self.recognize_from_image(image)
        
        except Exception as e:
            logger.error(f"❌ 图片下载/识别失败 {image_url}: {e}")
            return []
    
    def recognize_from_image(self, image):
        """
        从PIL Image对象识别文字
        :param image: PIL Image对象
        :return: 识别结果列表 [{'text': '文字', 'confidence': 置信度, 'box': 坐标}]
        """
        try:
            # 转换为RGB模式（如果是RGBA）
            if image.mode == 'RGBA':
                image = image.convert('RGB')
            
            # 保存为临时文件（PaddleOCR需要文件路径）
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp:
                image.save(tmp.name, 'PNG')
                tmp_path = tmp.name
            
            try:
                # 执行OCR识别
                return self._run_with_fallback(tmp_path)
            
            finally:
                # 清理临时文件
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
        
        except Exception as e:
            logger.error(f"❌ OCR识别失败: {e}")
            return []
    
    def recognize_to_text(self, image_url, headers=None, separator='\n'):
        """
        简化接口：直接返回拼接后的文字
        :param image_url: 图片链接
        :param headers: 请求头
        :param separator: 文字拼接分隔符
        :return: 识别出的完整文字
        """
        results = self.recognize_from_url(image_url, headers)
        
        # 按置信度过滤并拼接
        texts = [
            str(item['text']) 
            for item in results 
            if item['confidence'] > 0.5  # 只保留置信度>50%的结果
        ]
        
        return separator.join(texts)

    def recognize_local_image(self, image_path):
        """
        识别本地图片文件
        :param image_path: 本地图片路径
        :return: 识别结果列表
        """
        try:
            return self._run_with_fallback(image_path)
        except Exception as e:
            logger.error(f"❌ 本地图片识别失败: {e}")
            return []


# 单例模式 - 全局OCR实例
_ocr_instance = None

def get_ocr_processor():
    """获取OCR处理器单例"""
    global _ocr_instance
    if _ocr_instance is None:
        _ocr_instance = OCRProcessor()
    return _ocr_instance