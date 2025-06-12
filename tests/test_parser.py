from datetime import date, datetime
from unittest.mock import patch, mock_open, Mock
import pytest


class TestSpimexParser:
    def test_ensure_date(self, parser):
        """Тест проверяет корректное преобразование даты."""
        dt = datetime(2023, 1, 1)
        assert parser._ensure_date(dt) == date(2023, 1, 1)

        d = date(2023, 1, 1)
        assert parser._ensure_date(d) == date(2023, 1, 1)

        with pytest.raises(ValueError):
            parser._ensure_date("2023-01-01")


    @patch('core.parser.requests.get')
    def test_get_total_pages(self, mock_get, parser):
        """Тест проверяет определение общего количества страниц пагинации."""
        mock_response = Mock()
        mock_response.text = """
        <div class="bx-pagination">
            <ul>
                <li class="bx-pag-next"><a>Next</a></li>
                <li><a>1</a></li>
                <li><a>2</a></li>
                <li class="bx-active"><a><span>3</span></a></li>
                <li class="bx-pag-prev"><a>Prev</a></li>
            </ul>
        </div>
        """
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        assert parser.get_total_pages() == 3

        mock_response.text = "<html><body>No pagination here</body></html>"
        assert parser.get_total_pages() == 1


    def test_parse_date_from_filename(self, parser):
        """Тест проверяет извлечение даты из имени файла."""
        filename = "oil_xls_20230101.xls"
        assert parser.parse_date_from_filename(filename) == date(2023, 1, 1)
        assert parser.parse_date_from_filename("invalid") is None


    @patch('core.parser.requests.get')
    @patch('core.parser.os.path.exists')
    def test_download_file(self, mock_exists, mock_get, parser):
        """Тест проверяет загрузку файла по URL."""
        mock_exists.return_value = False
        mock_response = Mock()
        mock_response.content = b"test content"
        mock_get.return_value = mock_response

        url = "https://example.com/oil_xls_20230101.xls"

        with patch('builtins.open', mock_open()) as mock_file:
            assert parser.download_file(url) is True
            mock_file().write.assert_called_with(b"test content")

        mock_exists.return_value = True
        assert parser.download_file(url) is True

        parser.config['start_date'] = date(2024, 1, 1)
        assert parser.download_file(url) is False


    @patch('core.parser.requests.get')
    def test_parse_page(self, mock_get, parser):
        """Тест проверяет парсинг ссылок на файлы с веб-страницы."""
        mock_response = Mock()
        mock_response.text = """
        <a href="/upload/reports/oil_xls/oil_xls_20230101.xls">File1</a>
        <a href="https://other.com/file.pdf">Other</a>   
        <a href="/upload/reports/oil_xls/oil_xls_20230102.xls?param=value">File2</a>
        """
        mock_get.return_value = mock_response

        expected = [
            "https://spimex.com/upload/reports/oil_xls/oil_xls_20230101.xls",
            "https://spimex.com/upload/reports/oil_xls/oil_xls_20230102.xls"
        ]
        assert parser.parse_page("https://example.com")  == expected

        parser._should_stop = True
        assert parser.parse_page("https://example.com")  == []


    @patch('core.parser.SpimexParser.get_total_pages')
    @patch('core.parser.SpimexParser.parse_page')
    @patch('core.parser.SpimexParser.download_file')
    def test_run(self, mock_download, mock_parse, mock_pages, parser):
        """Тест проверяет полный цикл работы парсера."""
        # Успешный запуск
        mock_pages.return_value = 2
        mock_parse.side_effect = [
            ["file1.xls", "file2.xls"],
            ["file3.xls"]
        ]
        mock_download.return_value = True

        assert parser.run() is True
        assert mock_parse.call_count == 2

        # Неуспешный запуск (ошибка при скачивании)
        mock_download.side_effect = [True, False]
        mock_pages.return_value = 3
        mock_parse.side_effect = [
            ["file1.xls", "file2.xls"],
            ["file3.xls"],
            ["file4.xls"]
        ]

        assert parser.run() is False
        assert mock_parse.call_count == 4