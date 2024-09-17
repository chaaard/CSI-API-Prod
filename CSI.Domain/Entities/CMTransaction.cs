using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class CMTransaction
    {
        public long Id { get; set; }
        public long Seq { get; set; }
        public string CustomerCode { get; set; } = string.Empty;
        public int Location {  get; set; }
        public decimal TransactionDate { get; set; }
        public string MembershipNo { get; set; } = string.Empty;
        public string RegisterNo {  get; set; } = string.Empty;
        public string CashierNo {  get; set; } = string.Empty;
        public string TrxNo { get; set; } = string.Empty;
        public string JobOrderNo { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public int Status { get; set; }
        public DateTime ModifiedDate { get; set; }
        public string? ModifiedBy { get; set; } = string.Empty;
        public string? CMInvoiceNo { get; set; } = string.Empty;
        public string? OrigInvoice { get; set; } = string.Empty;
        public string? ReferenceNo { get; set; } = string.Empty;
        public string? FileName { get; set; } = string.Empty;
        public string? GeneratedBy { get; set; } = string.Empty;
        public DateTime? GeneratedDate { get; set; }
        public bool IsDeleted {  get; set; }
    }

    public class VW_CMTransactions
    {
        public long Id { get; set; }
        public long Seq { get; set; }
        public string CustomerCode { get; set; } = string.Empty;
        public string? CustomerName { get; set; }
        public decimal TransactionDate { get; set; }
        public int Location { get; set; }
        public string MembershipNo { get; set; } = string.Empty;
        public string CashierNo { get; set; } = string.Empty;
        public string RegisterNo { get; set; } = string.Empty;
        public string TransactionNo { get; set; } = string.Empty;
        public string JobOrderNo { get; set; } = string.Empty;
        public decimal Amount { get; set; }
        public int Status { get; set; }
        public bool IsDeleted { get; set; }
    }
}
