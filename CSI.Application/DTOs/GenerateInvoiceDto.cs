using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class GenerateInvoiceDto
    {
        public int Id { get; set; }
        public int Club { get; set; }
        public string CustomerCode { get; set; } = string.Empty;
        public string CustomerNo { get; set; } = string.Empty;
        public string CustomerName { get; set; } = string.Empty;
        public string InvoiceNo { get; set; } = string.Empty;
        public DateTime? InvoiceDate { get; set; }
        public DateTime? TransactionDate { get; set; }
        public string Location { get; set; } = string.Empty;
        public string ReferenceNo { get; set; } = string.Empty;
        public decimal? InvoiceAmount { get; set; }
        public string? FileName { get; set; } = string.Empty;
    }
}
