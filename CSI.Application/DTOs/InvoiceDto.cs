using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.DTOs
{
    public class InvoiceDto
    {
        public string HDR_TRX_NUMBER { get; set; } = string.Empty;
        public DateTime? HDR_TRX_DATE { get; set; }
        public string HDR_PAYMENT_TYPE { get; set; } = string.Empty;
        public string HDR_BRANCH_CODE { get; set; } = string.Empty;
        public string HDR_CUSTOMER_NUMBER { get; set; } = string.Empty;
        public string HDR_CUSTOMER_SITE { get; set; } = string.Empty;
        public string HDR_PAYMENT_TERM { get; set; } = string.Empty;
        public string HDR_BUSINESS_LINE { get; set; } = string.Empty;
        public string HDR_BATCH_SOURCE_NAME { get; set; } = string.Empty;
        public DateTime? HDR_GL_DATE { get; set; }
        public string HDR_SOURCE_REFERENCE { get; set; } = string.Empty;
        public string DTL_LINE_DESC { get; set; } = string.Empty;
        public int DTL_QUANTITY { get; set; }
        public decimal? DTL_AMOUNT { get; set; }
        public string DTL_VAT_CODE { get; set; } = string.Empty;
        public string DTL_CURRENCY { get; set; } = string.Empty;
        public string INVOICE_APPLIED { get; set; } = string.Empty;
        public string FILENAME { get; set; } = string.Empty;
        public string REMARKS { get; set; } = string.Empty;
        public DateTime? ORIG_TRAN_DATE { get; set; }
    }
}
