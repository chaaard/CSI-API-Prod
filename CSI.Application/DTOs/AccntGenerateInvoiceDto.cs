using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AccntGenerateInvoiceDto
    {
        public int Id { get; set; }
        public string CustomerId { get; set; } = string.Empty;
        public DateTime? Date { get; set; }
        public string Location { get; set; } = string.Empty;
        public int SubmitStatus { get; set; }
        public int? LocationId { get; set; }
        public bool? IsGenerated { get; set; }
    }
}
