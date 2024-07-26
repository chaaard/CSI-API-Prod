using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AdjustmentParams
    {
        public int PageNumber { get; set; }
        public int PageSize { get; set; }
        public string? SearchQuery { get; set; }
        public string? ColumnToSort { get; set; }
        public string? OrderBy { get; set; }
        public List<string> dates { get; set; } = new List<string>();
        public List<string> memCode { get; set; } = new List<string>();
        public List<string> memCode2 { get; set; } = new List<string>();
        public string? userId { get; set; } = string.Empty;
        public List<int> storeId { get; set; } = new List<int>();
        public string? remarks { get; set; } = string.Empty;
    }
}
