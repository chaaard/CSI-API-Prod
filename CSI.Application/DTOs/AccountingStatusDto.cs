using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class AccountingStatusDto
    {
        public string Status { get; set; } = string.Empty;
        public int Count { get; set; } 
        public decimal TotalAmount { get; set; }
    }
}
