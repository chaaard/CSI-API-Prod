using CSI.Application.DTOs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Helper
{
    public class DocumentHelper
    {
        public InvoiceDto InvoiceMapper(string trxNo,DateTime trxDate,string paymentType,DateTime glDate,string lineDesc,
            decimal amount,string invApplied, DateTime? origTranDate,string? branchCode = null,string? customerNo = null,string? customerSite = null,string? filename = null,string? remarks = null)
        {
            var invoice = new InvoiceDto();
            invoice.HDR_TRX_NUMBER = trxNo;
            invoice.HDR_TRX_DATE = trxDate;
            invoice.HDR_PAYMENT_TYPE = paymentType;
            invoice.HDR_BRANCH_CODE = string.IsNullOrEmpty(branchCode) ? string.Empty:branchCode;
            invoice.HDR_CUSTOMER_NUMBER = string.IsNullOrEmpty(customerNo) ? string.Empty : customerNo;
            invoice.HDR_CUSTOMER_SITE = string.IsNullOrEmpty(customerSite) ? string.Empty : customerSite;
            invoice.HDR_PAYMENT_TERM = "0";
            invoice.HDR_BUSINESS_LINE = "1";
            invoice.HDR_BATCH_SOURCE_NAME = "POS";
            invoice.HDR_GL_DATE = glDate;
            invoice.HDR_SOURCE_REFERENCE = paymentType;
            invoice.DTL_LINE_DESC = lineDesc;
            invoice.DTL_QUANTITY = 1;
            invoice.DTL_AMOUNT = amount;
            invoice.DTL_VAT_CODE = "";
            invoice.DTL_CURRENCY = "PHP";
            invoice.INVOICE_APPLIED = invApplied;
            invoice.FILENAME = string.IsNullOrEmpty(filename) ? string.Empty : filename;
            invoice.REMARKS = string.IsNullOrEmpty(remarks) ? string.Empty : remarks;
            invoice.ORIG_TRAN_DATE = origTranDate;
            return invoice;
        }
    }
}
