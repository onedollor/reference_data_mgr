using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ReferenceDataApi.Models;

namespace ReferenceDataApi.Services
{
    public class CsvDetector : ICsvDetector
    {
        private readonly ILogger _logger;

        public CsvDetector(ILogger logger)
        {
            _logger = logger;
        }

        public FormatDetectionResponse DetectFormat(string filePath)
        {
            try
            {
                if (!File.Exists(filePath))
                {
                    return new FormatDetectionResponse
                    {
                        Detected = false,
                        Confidence = 0.0,
                        Message = "File not found"
                    };
                }

                // Read first few lines for analysis - .NET Framework 4.5 compatible
                var lines = new List<string>();
                using (var reader = new StreamReader(filePath))
                {
                    var lineCount = 0;
                    string line;
                    while ((line = reader.ReadLine()) != null && lineCount < 10)
                    {
                        lines.Add(line);
                        lineCount++;
                    }
                }

                if (lines.Count == 0)
                {
                    return new FormatDetectionResponse
                    {
                        Detected = false,
                        Confidence = 0.0,
                        Message = "File is empty"
                    };
                }

                // Simple detection logic - .NET Framework 4.5 compatible
                var detectedDelimiters = DetectDelimiters(lines);
                var primaryDelimiter = detectedDelimiters.FirstOrDefault() ?? "|";
                
                var hasHeader = DetectHeader(lines, primaryDelimiter);
                var hasTrailer = DetectTrailer(lines);
                
                var columnCount = EstimateColumns(lines[0], primaryDelimiter);
                var rowCount = lines.Count;

                var confidence = CalculateConfidence(lines, primaryDelimiter);

                // Extract column names from header if detected
                var columnNames = ExtractColumnNames(lines, primaryDelimiter, hasHeader);
                
                // Extract sample data rows (skip header if present, take up to 3 rows)
                var sampleRows = ExtractSampleRows(lines, primaryDelimiter, hasHeader, 3);

                var response = new FormatDetectionResponse
                {
                    Detected = confidence > 0.5,
                    Confidence = confidence,
                    Suggestions = new CsvFormatSuggestion
                    {
                        HeaderDelimiter = primaryDelimiter,
                        ColumnDelimiter = primaryDelimiter,
                        RowDelimiter = "\r\n",
                        TextQualifier = "\"",
                        SkipLines = 0,
                        TrailerLine = hasTrailer ? "yes" : "",
                        LoadMode = "full"
                    },
                    detected_format = new DetectedFormat
                    {
                        header_delimiter = primaryDelimiter,
                        column_delimiter = primaryDelimiter,
                        row_delimiter = "\r\n",
                        text_qualifier = "\"",
                        skip_lines = 0,
                        trailer_line = hasTrailer ? "yes" : "",
                        load_mode = "full"
                    },
                    Analysis = new FormatAnalysis
                    {
                        SampleSize = lines.Count,
                        DetectedDelimiters = detectedDelimiters,
                        DetectedEncodings = new List<string> { "UTF-8" }, // Simplified
                        HasHeader = hasHeader,
                        HasTrailer = hasTrailer,
                        EstimatedRows = rowCount,
                        EstimatedColumns = columnCount,
                        Columns = columnNames,
                        SampleRows = sampleRows
                    },
                    Message = confidence > 0.5 ? "Format detected successfully" : "Format detection uncertain"
                };

                return response;
            }
            catch (Exception ex)
            {
                _logger.LogError("csv_detection_error", "CSV format detection failed: " + ex.Message);
                
                return new FormatDetectionResponse
                {
                    Detected = false,
                    Confidence = 0.0,
                    Message = "Detection failed: " + ex.Message
                };
            }
        }

        private List<string> DetectDelimiters(List<string> lines)
        {
            var delimiters = new List<string> { "|", ",", ";", "\t" };
            var delimiterCounts = new Dictionary<string, int>();

            // .NET Framework 4.5 compatible initialization
            foreach (var delimiter in delimiters)
            {
                delimiterCounts[delimiter] = 0;
            }

            // Count occurrences of each delimiter
            foreach (var line in lines.Take(5)) // Check first 5 lines
            {
                foreach (var delimiter in delimiters)
                {
                    var count = line.Split(new string[] { delimiter }, StringSplitOptions.None).Length - 1;
                    delimiterCounts[delimiter] += count;
                }
            }

            // Sort by count descending - .NET Framework 4.5 compatible
            var sortedDelimiters = new List<string>();
            foreach (var kvp in delimiterCounts.OrderByDescending(x => x.Value))
            {
                if (kvp.Value > 0)
                {
                    sortedDelimiters.Add(kvp.Key);
                }
            }

            return sortedDelimiters;
        }

        private bool DetectHeader(List<string> lines, string delimiter)
        {
            if (lines.Count < 2)
                return false;

            // Simple heuristic: if first line has different pattern than second line
            var firstLineParts = lines[0].Split(new string[] { delimiter }, StringSplitOptions.None);
            var secondLineParts = lines[1].Split(new string[] { delimiter }, StringSplitOptions.None);

            // If column counts differ, likely has header
            if (firstLineParts.Length != secondLineParts.Length)
                return true;

            // Check if first line contains more text (headers are usually descriptive)
            var firstLineTextRatio = CalculateTextRatio(firstLineParts);
            var secondLineTextRatio = CalculateTextRatio(secondLineParts);

            return firstLineTextRatio > secondLineTextRatio;
        }

        private bool DetectTrailer(List<string> lines)
        {
            if (lines.Count < 2)
                return false;

            var lastLine = lines[lines.Count - 1];
            
            // Simple heuristic: trailer lines often contain totals or summary info
            var trailerKeywords = new string[] { "total", "sum", "count", "end", "footer" };
            
            foreach (var keyword in trailerKeywords)
            {
                if (lastLine.ToLower().Contains(keyword))
                    return true;
            }

            return false;
        }

        private int EstimateColumns(string line, string delimiter)
        {
            if (string.IsNullOrEmpty(line))
                return 0;

            return line.Split(new string[] { delimiter }, StringSplitOptions.None).Length;
        }

        private double CalculateTextRatio(string[] parts)
        {
            if (parts == null || parts.Length == 0)
                return 0.0;

            var textCount = 0;
            foreach (var part in parts)
            {
                double numericValue;
                if (!double.TryParse(part.Trim(), out numericValue))
                {
                    textCount++;
                }
            }

            return (double)textCount / parts.Length;
        }

        private List<string> ExtractColumnNames(List<string> lines, string delimiter, bool hasHeader)
        {
            if (lines.Count == 0)
                return new List<string>();

            if (hasHeader && lines.Count > 0)
            {
                // Use the first line as column headers
                var headerLine = lines[0];
                var columnNames = headerLine.Split(new string[] { delimiter }, StringSplitOptions.None);
                
                // Clean up column names - remove quotes and trim whitespace
                var cleanedNames = new List<string>();
                foreach (var name in columnNames)
                {
                    var cleanName = name.Trim().Trim('"').Trim('\'').Trim();
                    if (string.IsNullOrEmpty(cleanName))
                    {
                        cleanName = "column_" + (cleanedNames.Count + 1);
                    }
                    cleanedNames.Add(cleanName);
                }
                
                return cleanedNames;
            }
            else
            {
                // Generate generic column names
                var columnCount = EstimateColumns(lines[0], delimiter);
                var columnNames = new List<string>();
                for (int i = 1; i <= columnCount; i++)
                {
                    columnNames.Add("column_" + i);
                }
                return columnNames;
            }
        }

        private List<List<string>> ExtractSampleRows(List<string> lines, string delimiter, bool hasHeader, int maxRows)
        {
            var sampleRows = new List<List<string>>();
            
            if (lines.Count == 0)
                return sampleRows;

            // Start from line 1 if has header, otherwise from line 0
            var startIndex = hasHeader ? 1 : 0;
            var rowCount = 0;

            for (int i = startIndex; i < lines.Count && rowCount < maxRows; i++)
            {
                var line = lines[i];
                if (string.IsNullOrWhiteSpace(line))
                    continue;

                try
                {
                    // Parse the line into columns
                    var fields = line.Split(new string[] { delimiter }, StringSplitOptions.None);
                    var cleanFields = new List<string>();
                    
                    foreach (var field in fields)
                    {
                        cleanFields.Add(field.Trim().Trim('"').Trim('\''));
                    }
                    
                    sampleRows.Add(cleanFields);
                    rowCount++;
                }
                catch
                {
                    // Skip lines that can't be parsed
                    continue;
                }
            }

            return sampleRows;
        }

        private double CalculateConfidence(List<string> lines, string delimiter)
        {
            if (lines.Count == 0)
                return 0.0;

            var consistencyScore = 0.0;
            var firstLineColumnCount = EstimateColumns(lines[0], delimiter);

            // Check consistency of column counts
            var consistentLines = 0;
            foreach (var line in lines)
            {
                var columnCount = EstimateColumns(line, delimiter);
                if (columnCount == firstLineColumnCount)
                {
                    consistentLines++;
                }
            }

            consistencyScore = (double)consistentLines / lines.Count;

            // Basic confidence calculation
            var confidence = consistencyScore * 0.8; // 80% weight on consistency
            
            if (firstLineColumnCount > 1)
            {
                confidence += 0.2; // 20% bonus for having multiple columns
            }

            return Math.Min(confidence, 1.0);
        }
    }
}