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
        public string? FilePath { get; set; }
        public string? Username { get; set; }
    }
    public class CreditMemoInvoiceDto
    {
        public string? UserId;
        public List<string>? Dates;
        public List<string>? MerchantCode;
        public List<int>? StoreId;
        public string? Action;
        public string? Username;
    }
}
