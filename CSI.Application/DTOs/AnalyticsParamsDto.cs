using CSI.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AnalyticsParamsDto
    {
        public int? Id { get; set; }
        public int? PageNumber { get; set; }
        public int? PageSize { get; set; }
        public string? SearchQuery { get; set; }
        public string? ColumnToSort { get; set; }
        public string? OrderBy { get; set; }
        public List<string>? dates { get; set; } = new List<string>();
        public List<string>? memCode { get; set; } = new List<string>();
        public string? userId { get; set; } = string.Empty;
        public List<int>? storeId { get; set; } = new List<int>();
        public List<string>? status { get; set; } = new List<string>();
        public string? orderNo { get; set; } = string.Empty;
        public bool? isView { get; set; }
        public string? action { get; set; } = string.Empty;
        public string fileName { get; set; } = string.Empty;
        public string? remarks { get; set; } = string.Empty;
        public string? selectedItem { get; set; } = string.Empty;
        public string? AutoChargeDate { get; set; } = string.Empty;
        public List<string>? merchantDetails { get; set; } = new List<string>();
    }
}
