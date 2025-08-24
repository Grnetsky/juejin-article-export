#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
掘金小册内容爬虫
功能：获取掘金小册的所有章节内容并合并为一个Markdown文件或拆分为多个文件
"""

import time
import logging
from pathlib import Path
from typing import Dict, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import configparser
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass
class BookletConfig:
    """小册配置类"""
    cookie: str
    book_id: str
    output_dir: str  # 输出目录路径
    max_workers: int = 5
    request_delay: float = 0.5
    auto_title: bool = True
    auto_all: bool = True
    merge_single_file: bool = True  # 新增：是否合并为单个文件


class JuejinAPI:
    """掘金API封装类"""

    BASE_URL = "https://api.juejin.cn"

    def __init__(self, cookie: str):
        self.session = self._create_session()
        self.headers = {
            'Cookie': cookie,
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Content-Type': 'application/json',
            'Referer': 'https://juejin.cn/'
        }

    def _create_session(self) -> requests.Session:
        """创建带重试机制的会话"""
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def get_booklet_sections(self, book_id: str) -> Tuple[Dict[str, str], str]:
        """获取小册章节列表和书籍标题"""
        url = f"{self.BASE_URL}/booklet_api/v1/booklet/get"
        payload = {"booklet_id": book_id}

        try:
            response = self.session.post(url, json=payload, headers=self.headers, timeout=10)
            response.raise_for_status()

            data = response.json()
            if data.get('err_no') != 0:
                raise ValueError(f"API返回错误: {data.get('err_msg', '未知错误')}")

            booklet_data = data.get('data', {})
            sections = booklet_data.get('sections', [])
            book_title = booklet_data.get('booklet', {}).get('base_info', {}).get('title', '未知小册')

            sections_dict = {section['draft_title']: section['section_id'] for section in sections}

            return sections_dict, book_title

        except requests.RequestException as e:
            logging.error(f"获取章节列表失败: {e}")
            raise

    def get_section_content(self, section_id: str) -> Optional[str]:
        """获取单个章节内容"""
        url = f"{self.BASE_URL}/booklet_api/v1/section/get"
        payload = {"section_id": section_id}

        try:
            response = self.session.post(url, json=payload, headers=self.headers, timeout=15)
            response.raise_for_status()

            data = response.json()
            if data.get('err_no') != 0:
                logging.warning(f"章节 {section_id} 获取失败: {data.get('err_msg', '未知错误')}")
                return None

            return data.get('data', {}).get('section', {}).get('markdown_show', '')

        except requests.RequestException as e:
            logging.error(f"获取章节 {section_id} 内容失败: {e}")
            return None

    def get_book_list(self):
        url = f"{self.BASE_URL}/booklet_api/v1/booklet/bookletshelflist"
        try:
            response = self.session.post(url, headers=self.headers, timeout=15)
            response.raise_for_status()

            data = response.json()
            book_list = data.get('data', [])
            return [item.get('booklet_id') for item in book_list]
        except requests.RequestException as e:
            logging.error(f"获取书架列表失败: {e}")
            return []


class BookletScraper:
    """小册爬虫主类"""

    def __init__(self, config: BookletConfig):
        self.config = config
        self.api = JuejinAPI(config.cookie)

        self.output_dir = Path(config.output_dir)
        self.book_output_path = None  # 单个文件路径 或 目录路径
        self.merge_single_file = config.merge_single_file

        self._setup_logging()
        self.logger = logging.getLogger(__name__)

    def _setup_logging(self) -> None:
        """设置日志配置"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('booklet_scraper.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

    def _sanitize_filename(self, filename: str) -> str:
        """清理文件名，移除不合法字符"""
        import re
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', filename)
        return safe_name[:100] if len(safe_name) > 100 else safe_name

    def _prepare_output_structure(self, book_title: str) -> Path:
        """准备输出结构：单文件 or 多文件目录"""
        safe_title = self._sanitize_filename(book_title)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        if self.merge_single_file:
            # 单文件模式
            file_path = self.output_dir / f"{safe_title}.md"
            if file_path.exists():
                file_path.unlink()
            return file_path
        else:
            # 多文件模式：创建子目录
            dir_path = self.output_dir / safe_title
            dir_path.mkdir(exist_ok=True)
            return dir_path

    def _write_single_file_header(self, book_title: str, sections: Dict[str, str]) -> None:
        """写入单文件的头部信息"""
        with open(self.book_output_path, 'w', encoding='utf-8') as f:
            f.write(f"# {book_title}\n\n")
            f.write(f"**小册ID**: {self.config.book_id}\n\n")
            f.write(f"**生成时间**: {time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"**章节总数**: {len(sections)}\n\n")
            f.write("---\n\n")

            f.write("## 目录\n\n")
            for i, title in enumerate(sections.keys(), 1):
                f.write(f"{i}. {title}\n")
            f.write("\n---\n")

    def _write_section_to_single_file(self, title: str, content: str) -> None:
        """写入章节到单个文件"""
        with open(self.book_output_path, 'a', encoding='utf-8') as f:
            if self.config.auto_title:
                f.write(f"\n\n# {title}\n\n")
            if content:
                f.write(content)
            else:
                f.write("*此章节内容获取失败*\n")
            f.write("\n\n")

    def _write_section_to_separate_file(self, title: str, content: str, index: int) -> None:
        """将章节保存为独立文件"""
        safe_title = self._sanitize_filename(title)
        file_name = f"{index:03d}_{safe_title}.md"
        file_path = self.book_output_path / file_name

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(f"# {title}\n\n")
            if content:
                f.write(content)
            else:
                f.write("*此章节内容获取失败*\n")

        self.logger.info(f"章节已保存: {file_path}")

    def _fetch_section_content(self, section_info: Tuple[str, str]) -> Tuple[str, str]:
        """获取单个章节内容（用于并发）"""
        title, section_id = section_info
        self.logger.info(f"正在获取章节: {title}")

        content = self.api.get_section_content(section_id)
        time.sleep(self.config.request_delay)

        return title, content

    def getBookList(self):
        if self.config.auto_all:
            return self.api.get_book_list()
        else:
            return [self.config.book_id]

    def scrape_booklet(self, book_id=None) -> None:
        """爬取小册内容"""
        try:
            self.logger.info("开始获取小册章节列表...")
            sections, book_title = self.api.get_booklet_sections(book_id or self.config.book_id)

            if not sections:
                self.logger.error("未获取到任何章节")
                return

            self.logger.info(f"小册标题: {book_title}")
            self.logger.info(f"共发现 {len(sections)} 个章节")

            # 准备输出结构
            self.book_output_path = self._prepare_output_structure(book_title)
            self.logger.info(f"输出路径: {self.book_output_path}")

            # 单文件模式：写入头部
            if self.merge_single_file:
                self._write_single_file_header(book_title, sections)

            # 并发获取内容
            self.logger.info("开始获取章节内容...")
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                section_items = list(sections.items())
                future_to_section = {
                    executor.submit(self._fetch_section_content, item): item
                    for item in section_items
                }

                results = {}
                for future in as_completed(future_to_section):
                    try:
                        title, content = future.result()
                        results[title] = content
                    except Exception as e:
                        section_info = future_to_section[future]
                        self.logger.error(f"处理章节 {section_info[0]} 时发生错误: {e}")
                        results[section_info[0]] = None

                success_count = 0
                for i, (title, _) in enumerate(section_items, 1):
                    content = results.get(title)

                    if self.merge_single_file:
                        self._write_section_to_single_file(title, content)
                    else:
                        self._write_section_to_separate_file(title, content, i)

                    if content:
                        success_count += 1
                        self.logger.info(f"✓ 章节 '{title}' 获取成功")
                    else:
                        self.logger.warning(f"✗ 章节 '{title}' 获取失败")

            self.logger.info(f"爬取完成！成功获取 {success_count}/{len(sections)} 个章节")
            self.logger.info(f"输出路径: {self.book_output_path.absolute()}")

        except Exception as e:
            self.logger.error(f"爬取过程中发生错误: {e}")
            raise


def load_config(config_file: str = 'config.ini') -> BookletConfig:
    """加载配置文件"""
    config = configparser.ConfigParser(interpolation=None)

    if not Path(config_file).exists():
        raise FileNotFoundError(f"配置文件 {config_file} 不存在")

    config.read(config_file, encoding='utf-8')

    return BookletConfig(
        cookie=config.get('userinfo', 'cookie'),
        book_id=config.get('book', 'book_id'),
        output_dir=config.get('out', 'file_path'),
        max_workers=config.getint('settings', 'max_workers', fallback=3),
        request_delay=config.getfloat('settings', 'request_delay', fallback=0.5),
        auto_title=config.getboolean('out', 'auto_title', fallback=True),
        auto_all=config.getboolean('book', 'auto_all', fallback=True),
        merge_single_file=config.getboolean('out', 'merge_single_file', fallback=True),  # 新增
    )


def main():
    try:
        config = load_config()
        scraper = BookletScraper(config)
        book_id_list = scraper.getBookList()
        for book_id in book_id_list:
            scraper.scrape_booklet(book_id)
    except Exception as e:
        print(f"程序执行失败: {e}")
        return 1
    return 0


if __name__ == '__main__':
    exit(main())