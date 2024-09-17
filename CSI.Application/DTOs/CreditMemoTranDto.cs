using CSI.Domain.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class CreditMemoTranDto
    {
        public VarianceMMS? Variance { get; set; }
        public List<CustomerTransactionDto>? CMTranList { get; set; }
    }
    public class CreditMemoDto
    {
        public string? Id {  get; set; }
        public List<CustomerTransactionDto>? CMTranList { get; set; }
        public string? SelectedDate { get; set; }
        public int Club { get; set; }
    }
}
