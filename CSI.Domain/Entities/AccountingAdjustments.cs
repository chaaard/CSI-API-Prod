﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Domain.Entities
{
    public class AccountingAdjustments
    {
        public int Id { get; set; }
        public int? AccountingAdjustmentTypeId { get; set; }
        public DateTime? NewTransactionDate { get; set; }
        public string? CashierName { get; set; } = string.Empty;
        public string? Agency { get; set; } = string.Empty;
        public string? AccountPaymentReferenceNo { get; set; } = string.Empty;
        public decimal? Amount { get; set; }
        public string? Remarks { get; set; } = string.Empty;
        public int? MatchId { get; set; }
        public int? AccountingAnalyticsId { get; set; }
        public int? AccountingProofListId { get; set; }
        public bool DeleteFlag { get; set; }
    }
}
