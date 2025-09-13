"""
CSV Format Auto-Detection Utility
Automatically detects CSV format parameters including delimiters, headers, and trailers
"""

import csv
import re
from typing import Dict, Any, List, Optional
import chardet
from collections import Counter

class CSVFormatDetector:
    """Detects CSV format parameters automatically"""

    def __init__(self):
        self.common_delimiters = [',', ';', '|', '\t']
        self.common_text_qualifiers = ['"', "'", '']
        self.common_row_delimiters = ['\n', '\r\n', '\r']

    def detect_format(self, file_path: str, sample_size: int = 8192) -> Dict[str, Any]:
        """
        Detect CSV format parameters from a file

        Args:
            file_path: Path to the CSV file
            sample_size: Number of bytes to read for detection

        Returns:
            Dictionary containing detected format parameters
        """
        try:
            # Read file with encoding detection
            with open(file_path, 'rb') as file:
                raw_data = file.read(sample_size)
                encoding_result = chardet.detect(raw_data)
                encoding = encoding_result['encoding'] if encoding_result['confidence'] > 0.7 else 'utf-8'

            # Read file content with detected encoding
            with open(file_path, 'r', encoding=encoding, errors='ignore') as file:
                content = file.read(sample_size)

            if not content.strip():
                raise ValueError("File appears to be empty")

            lines = content.split('\n')
            if len(lines) < 2:
                raise ValueError("File must have at least 2 lines for detection")

            # Detect format parameters
            column_delimiter = self._detect_column_delimiter(content, lines)
            row_delimiter = self._detect_row_delimiter(content)
            text_qualifier = self._detect_text_qualifier(content, column_delimiter)
            has_header = self._detect_header(lines, column_delimiter, text_qualifier)

            # For trailer detection we must inspect the real last non-empty lines of the FULL file, not just the sample window.
            trailer_line = None
            has_trailer = False
            try:
                with open(file_path, 'r', encoding=encoding, errors='ignore') as f_full:
                    full_lines = [ln.rstrip('\n\r') for ln in f_full]
                # Collect last 200 non-empty lines to minimize memory while covering edge cases (footers sometimes multi-line)
                non_empty_full = [l for l in full_lines if l.strip()]
                if len(non_empty_full) >= 2:
                    last_line_full = non_empty_full[-1]
                    prev_line_full = non_empty_full[-2]
                    # Derive typical column count from first 100 non-empty data lines (excluding potential header and last line)
                    sample_for_mode = []
                    start_index = 1 if has_header else 0
                    for l in non_empty_full[start_index: min(start_index+100, len(non_empty_full)-1)]:
                        try:
                            sample_for_mode.append(len(self._parse_row(l, column_delimiter, text_qualifier)))
                        except Exception:
                            pass
                    typical_cols = None
                    if sample_for_mode:
                        try:
                            from statistics import mode
                            typical_cols = mode(sample_for_mode)
                        except Exception:
                            typical_cols = Counter(sample_for_mode).most_common(1)[0][0]
                    try:
                        last_cols = len(self._parse_row(last_line_full, column_delimiter, text_qualifier))
                        prev_cols = len(self._parse_row(prev_line_full, column_delimiter, text_qualifier))
                    except Exception:
                        last_cols = prev_cols = None
                    # Trailer per business rule: last line column count differs from previous AND previous matches typical columns
                    if last_cols is not None and prev_cols is not None and last_cols != prev_cols:
                        if typical_cols is None or prev_cols == typical_cols:
                            has_trailer = True
                            trailer_line = last_line_full
            except Exception:
                # Fail silent; fallback to sample-based basic rule
                if not has_trailer:
                    has_trailer, trailer_line = self._detect_trailer(lines, column_delimiter, text_qualifier)

            # Parse first few rows to get column info
            sample_data = self._parse_sample_data(lines[:10], column_delimiter, text_qualifier)

            # Just store the actual trailer line text if detected

            result = {
                'encoding': encoding,
                'encoding_confidence': encoding_result['confidence'],
                'column_delimiter': column_delimiter,
                'row_delimiter': row_delimiter,
                'text_qualifier': text_qualifier,
                'has_header': has_header,
                'has_trailer': has_trailer,
                'estimated_columns': len(sample_data[0]) if sample_data else 0,
                'sample_rows': len(lines),
                'detection_confidence': self._calculate_confidence(content, column_delimiter, text_qualifier),
                'sample_data': sample_data[:3],  # First 3 rows as sample
                'header_delimiter': column_delimiter,  # Same as column delimiter typically
                'skip_lines': 0,  # Can be enhanced to detect skip lines
                'trailer_line': trailer_line if has_trailer else None
            }

            return result

        except Exception as e:
            return {
                'error': str(e),
                'column_delimiter': ',',
                'row_delimiter': '\n',
                'text_qualifier': '"',
                'has_header': True,
                'has_trailer': False,
                'header_delimiter': ',',
                'skip_lines': 0,
                'detection_confidence': 0.0
            }

    def _detect_column_delimiter(self, content: str, lines: List[str]) -> str:
        """Detect the column delimiter"""
        delimiter_scores = {}

        for delimiter in self.common_delimiters:
            score = 0
            delimiter_counts = []

            # Check consistency across lines
            for line in lines[:10]:  # Check first 10 lines
                if line.strip():
                    count = line.count(delimiter)
                    delimiter_counts.append(count)

            if delimiter_counts:
                # Score based on consistency and frequency
                avg_count = sum(delimiter_counts) / len(delimiter_counts)
                consistency = 1.0 - (max(delimiter_counts) - min(delimiter_counts)) / (max(delimiter_counts) + 1)
                score = avg_count * consistency
                delimiter_scores[delimiter] = score

        # Return delimiter with highest score
        if delimiter_scores:
            best_delimiter = max(delimiter_scores.keys(), key=lambda x: delimiter_scores[x])
            return best_delimiter

        return ','  # Default fallback

    def _detect_row_delimiter(self, content: str) -> str:
        """Detect the row delimiter"""
        crlf_count = content.count('\r\n')
        lf_count = content.count('\n') - crlf_count
        cr_count = content.count('\r') - crlf_count

        counts = {'\r\n': crlf_count, '\n': lf_count, '\r': cr_count}

        # Return the most common row delimiter
        best_delimiter = max(counts.keys(), key=lambda x: counts[x])
        return best_delimiter if counts[best_delimiter] > 0 else '\n'

    def _detect_text_qualifier(self, content: str, column_delimiter: str) -> str:
        """Detect the text qualifier"""
        # Look for quoted fields
        patterns = [
            rf'{re.escape(column_delimiter)}"[^"]*"',  # ,"text"
            rf'{re.escape(column_delimiter)}\'[^\']*\'',  # ,'text'
            rf'^"[^"]*"',  # "text" at start
            rf'^\'[^\']*\'',  # 'text' at start
        ]

        quote_scores = {'"': 0, "'": 0, '': 0}

        for pattern in patterns:
            if '"' in pattern:
                quote_scores['"'] += len(re.findall(pattern, content, re.MULTILINE))
            elif "'" in pattern:
                quote_scores["'"] += len(re.findall(pattern, content, re.MULTILINE))

        # Check for fields that contain the delimiter (would need quotes)
        delimiter_in_field_pattern = rf'[^{re.escape(column_delimiter)}]*{re.escape(column_delimiter)}[^{re.escape(column_delimiter)}]*'
        potential_quoted_fields = len(re.findall(delimiter_in_field_pattern, content))

        if quote_scores['"'] > quote_scores["'"]:
            return '"'
        elif quote_scores["'"] > 0:
            return "'"
        elif potential_quoted_fields > 0:
            return '"'  # Default to double quotes if fields likely contain delimiters

        return ''  # No text qualifier needed

    def _detect_header(self, lines: List[str], column_delimiter: str, text_qualifier: str) -> bool:
        """Detect if the file has a header row"""
        if len(lines) < 2:
            return True  # Assume header if only one line

        try:
            # Parse first two rows
            first_row = self._parse_row(lines[0], column_delimiter, text_qualifier)
            second_row = self._parse_row(lines[1], column_delimiter, text_qualifier)

            if len(first_row) != len(second_row):
                return True  # Different column counts suggest header

            # Check data types - headers are usually strings, data has mixed types
            first_row_types = self._analyze_data_types(first_row)
            second_row_types = self._analyze_data_types(second_row)

            # If first row is all strings and second has numbers, likely header
            if (first_row_types.count('string') == len(first_row) and
                second_row_types.count('number') > 0):
                return True

            # Check for common header patterns
            header_indicators = ['id', 'name', 'date', 'time', 'code', 'description', 'value', 'amount']
            first_row_lower = [str(cell).lower().strip() for cell in first_row]

            header_matches = sum(1 for cell in first_row_lower if any(indicator in cell for indicator in header_indicators))
            if header_matches > len(first_row) * 0.3:  # 30% of columns match header patterns
                return True

            return False  # Probably no header

        except:
            return True  # Default to assuming header on parse errors

    def _detect_trailer(self, lines: List[str], column_delimiter: str, text_qualifier: str) -> tuple[bool, Optional[str]]:
        """Simple trailer detection per spec: examine last non-empty line.
        If its column count differs from the line immediately above (which matches typical data),
        classify it as trailer.
        Returns (has_trailer, trailer_line_text).
        """
        non_empty = [l for l in lines if l.strip()]
        if len(non_empty) < 2:
            return False, None
        last_line = non_empty[-1]
        prev_line = non_empty[-2]
        try:
            last_cols = len(self._parse_row(last_line, column_delimiter, text_qualifier))
            prev_cols = len(self._parse_row(prev_line, column_delimiter, text_qualifier))
        except Exception:
            return False, None
        if prev_cols > 0 and last_cols != prev_cols:
            return True, last_line
        return False, None
    def _parse_row(self, line: str, delimiter: str, text_qualifier: str) -> List[str]:
        """Parse a single row with given format parameters using proper CSV parsing"""
        import io
        # Use Python's csv module for proper parsing
        reader = csv.reader(
            io.StringIO(line),
            delimiter=delimiter,
            quotechar=text_qualifier if text_qualifier else None
        )
        try:
            return next(reader)
        except StopIteration:
            return []

    def _parse_sample_data(self, lines: List[str], delimiter: str, text_qualifier: str) -> List[List[str]]:
        """Parse sample data rows"""
        sample_data = []
        for line in lines:
            if line.strip():
                try:
                    row = self._parse_row(line, delimiter, text_qualifier)
                    sample_data.append(row)
                except:
                    continue
        return sample_data

    def _analyze_data_types(self, row: List[str]) -> List[str]:
        """Analyze data types in a row"""
        types = []
        for cell in row:
            cell_str = str(cell).strip()
            if not cell_str:
                types.append('empty')
            elif cell_str.replace('.', '').replace('-', '').replace('+', '').isdigit():
                types.append('number')
            elif re.match(r'^\d{4}-\d{2}-\d{2}', cell_str):
                types.append('date')
            else:
                types.append('string')
        return types

    def _calculate_confidence(self, content: str, delimiter: str, text_qualifier: str) -> float:
        """Calculate confidence score for the detection"""
        score = 0.5  # Base score

        # Check delimiter consistency
        lines = content.split('\n')[:10]
        delimiter_counts = [line.count(delimiter) for line in lines if line.strip()]
        if delimiter_counts:
            consistency = 1.0 - (max(delimiter_counts) - min(delimiter_counts)) / (max(delimiter_counts) + 1)
            score += consistency * 0.3

        # Check if quotes are balanced
        if text_qualifier:
            quote_count = content.count(text_qualifier)
            if quote_count % 2 == 0:  # Even number suggests balanced quotes
                score += 0.2
        return min(score, 1.0)