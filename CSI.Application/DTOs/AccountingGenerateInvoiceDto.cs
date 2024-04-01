using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AccountingGenerateInvoiceDto
    {
        public string memCode { get; set; } = string.Empty;
        public DateTime date { get; set; }
    }
}
