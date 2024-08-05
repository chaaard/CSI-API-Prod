using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class PaginationDto
    {
        public int? Id { get; set; }
        public int PageNumber { get; set; }
        public int PageSize { get; set; }
        public string? SearchQuery { get; set; }
        public string? ColumnToSort { get; set; }
        public string? OrderBy { get; set; }
        public int? CategoryId { get; set; }
        public bool? IsVisible { get; set; }
        public bool ByMerchant { get; set; }
        public bool? IsAllVisible { get; set; }
        public string? FromPage { get; set; }
    }
}
