using AutoMapper;
using Azure.Core;
using CSI.Application.DTOs;
using CSI.Application.Enums;
using CSI.Application.Helper;
using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Services
{
    public class CreditMemoService : ICreditMemoService
    {
        private readonly AppDBContext _dbContext;
        private readonly IConfiguration _configuration;
        private readonly IMapper _mapper;
        private readonly IDbContextFactory<AppDBContext> _contextFactory;
        private readonly IAnalyticsService _analyticsService;
        private readonly LinkedServerOptions _linkedServerOptions;
        private readonly DocumentHelper _documentHelper;


        public CreditMemoService(AppDBContext dbContext, IConfiguration configuration, IMapper mapper, IDbContextFactory<AppDBContext> contextFactory,
             IOptions<LinkedServerOptions> linkedServerOptions, IAnalyticsService analyticsService, DocumentHelper documentHelper)
        {
            _dbContext = dbContext;
            _configuration = configuration;
            _mapper = mapper;
            _contextFactory = contextFactory;
            _linkedServerOptions = linkedServerOptions.Value;
            _analyticsService = analyticsService;
            _documentHelper = documentHelper;
        }

        #region Getting CM Variance and Records
        public async Task<CreditMemoTranDto> GetCMVariance(VarianceParams variance)
        {
            return await RetriveCreditMemo(variance);
        }
        #endregion

        #region Update Customer Code and Job Order No 
        public async Task<bool> UpdateCustCreditMemo(CustomerTransactionDto custDto) // Update Status to Pending
        {
            bool result = false;
            var locateId = await _dbContext.CMTransaction.Where(x => x.Id == custDto.Id).FirstOrDefaultAsync();
            try
            {
                if (locateId != null)
                {
                    locateId.CustomerCode = custDto.CustomerCode ?? string.Empty;
                    locateId.JobOrderNo = custDto?.JobOrderNo ?? string.Empty;
                    locateId.Status = (int)StatusEnums.PENDING;
                    locateId.ModifiedBy = custDto?.ModifiedBy;
                    locateId.ModifiedDate = DateTime.Now;

                    //Updates the MMS
                    var updateResult = await UpdateCreditMemoMMS(custDto);
                    if (updateResult)
                    {
                        result = true;
                        await _dbContext.SaveChangesAsync();
                    }
                }
            }
            catch (Exception)
            {
                throw;
            }
            return result;
        }
        #endregion

        #region Update Credit Memo Status and AO File Generation
        public async Task<bool> UpdateCreditMemoStatus(CreditMemoDto custTranList) //Submit all
        {
            var result = false;
            var invoiceAnalytics = new List<InvoiceDto>();
            var invoiceNo = string.Empty;
            var content = new StringBuilder();
            var filename = "SN" + DateTime.Now.ToString("MMddyy_hhmmss") + ".A01";
            var analyticsList = _dbContext.Analytics.Where(x => x.TransactionDate == DateTime.Parse(custTranList.SelectedDate)).ToList();
            var storeList = await _analyticsService.GetLocations();
            var custCodeList = await _dbContext.CustomerCodes.ToListAsync();
            try
            {
                //loop
                foreach (var item in custTranList.CMTranList)
                {
                    var origInvoice = _dbContext.GenerateInvoice.Where(x => x.CustomerCode == item.CustomerCode && x.Club == item.Club && x.TransactionDate == DateTime.Parse(custTranList.SelectedDate))
                            .Select(x => new { x.InvoiceNo, x.ReferenceNo }).FirstOrDefault();
                    var lastCmInvoice = _dbContext.CMTransaction.OrderByDescending(i => i.Id).Select(x => new { x.CMInvoiceNo }).FirstOrDefault();

                    //cmInvoice length = 12;
                    var newInvoiceNo = string.IsNullOrEmpty(lastCmInvoice.CMInvoiceNo) ? 00000000001 : int.Parse(lastCmInvoice.CMInvoiceNo.Substring(2, lastCmInvoice.CMInvoiceNo.Length)) + 1;
                    var custId = _dbContext.CMTransaction.Where(x => x.Id == item.Id).FirstOrDefault();
                    if (custId != null)
                    {
                        var formattedCmInvoice = newInvoiceNo.ToString().Length < 2 ? newInvoiceNo.ToString($"D10") : newInvoiceNo.ToString();
                        custId.CMInvoiceNo = "CM" + formattedCmInvoice;
                        custId.FileName = filename;
                        custId.OrigInvoice = origInvoice?.InvoiceNo;
                        custId.GeneratedDate = DateTime.Now;
                        custId.GeneratedBy = custTranList.Id;
                        custId.Status = (int)StatusEnums.SUBMITTED;
                        _dbContext.SaveChanges();
                    }
                }

                //file generation
                DateTime.TryParse(custTranList.SelectedDate, out DateTime selectedDate);
                var formattedSelectedDate = selectedDate.ToString("yyMMdd");
                var cmCustPerBranchList = _dbContext.CMTransaction.Where(c => c.TransactionDate.ToString() == formattedSelectedDate).GroupBy(g => new
                {
                    g.CustomerCode,
                    g.JobOrderNo,
                    g.CMInvoiceNo,
                    g.OrigInvoice,
                    g.Location,
                    g.TransactionDate,
                }).Select(x => new
                {
                    CustomerCode = x.Key.CustomerCode,
                    JobOrderNo = x.Key.JobOrderNo,
                    CMInvoice = string.IsNullOrEmpty(x.Key.CMInvoiceNo) ? string.Empty : x.Key.CMInvoiceNo,
                    OrigInvoice = x.Key.OrigInvoice,
                    Location = x.Key.Location,
                    TransactionDate = x.Key.TransactionDate,
                    TotalAmount = x.Sum(c => c.Amount)
                }).ToList();
                foreach (var i in cmCustPerBranchList)
                {
                    var formattedDate = DateTime.ParseExact(i.TransactionDate.ToString(), "yyMMdd", CultureInfo.InvariantCulture);
                    var getShortName = storeList.Where(x => x.LocationCode == i.Location).Select(n => new { n.ShortName }).FirstOrDefault();
                    var getCustomerNo = custCodeList.Where(x => x.CustomerCode == i.CustomerCode).Select(c => new { c.CustomerNo }).FirstOrDefault();
                    var formatCustomerNo = getCustomerNo.CustomerNo.Replace("P", "").Trim();
                    var getReference = await _dbContext.Reference.Where(x => x.CustomerNo == formatCustomerNo).Select(n => new { n.MerchReference }).FirstOrDefaultAsync();
                    var referenceNo = getReference.MerchReference + i.Location + formattedDate.ToString("MMddyy") + "-" + cmCustPerBranchList.Count();
                    var updateCMInvTblRef = _dbContext.CMTransaction.Where(x => x.CustomerCode == i.CustomerCode && x.TransactionDate == i.TransactionDate).ToList();
                    foreach (var cust in updateCMInvTblRef)
                    {
                        cust.ReferenceNo = referenceNo;
                        _dbContext.SaveChanges();
                    }
                    var invoice = _documentHelper.InvoiceMapper(i.CMInvoice,formattedDate,"CM",formattedDate,referenceNo,i.TotalAmount,i.OrigInvoice, 
                        getShortName?.ShortName,getCustomerNo.CustomerNo,getShortName?.ShortName,filename, "CREDIT MEMO INVOICE");
                    var format = new
                    {
                        HDR_TRX_NUMBER = i.CMInvoice,
                        HDR_TRX_DATE = formattedDate.ToString("dd-MMM-yyyy"),
                        HDR_PAYMENT_TYPE = "CM",
                        HDR_BRANCH_CODE = getShortName.ShortName ?? "",
                        HDR_CUSTOMER_NUMBER = getCustomerNo.CustomerNo,
                        HDR_CUSTOMER_SITE = getShortName.ShortName ?? "",
                        HDR_PAYMENT_TERM = "0",
                        HDR_BUSINESS_LINE = "1",
                        HDR_BATCH_SOURCE_NAME = "POS",
                        HDR_GL_DATE = formattedDate.ToString("dd-MMM-yyyy"),
                        HDR_SOURCE_REFERENCE = "CM",
                        DTL_LINE_DESC = referenceNo,
                        DTL_QUANTITY = 1,
                        DTL_AMOUNT = i.TotalAmount,
                        DTL_VAT_CODE = "",
                        DTL_CURRENCY = "PHP",
                        INVOICE_APPLIED = i.OrigInvoice,
                        FILENAME = filename,
                    };
                    invoiceAnalytics.Add(invoice);
                    invoiceNo = format.HDR_TRX_NUMBER;
                    string line =
                        $"{format.HDR_TRX_NUMBER}|" +
                        $"{format.HDR_TRX_DATE}|" +
                        $"{format.HDR_PAYMENT_TYPE}|" +
                        $"{format.HDR_BRANCH_CODE}|" +
                        $"{format.HDR_CUSTOMER_NUMBER}|" +
                        $"{format.HDR_CUSTOMER_SITE}|" +
                        $"{format.HDR_PAYMENT_TERM}|" +
                        $"{format.HDR_BUSINESS_LINE}|" +
                        $"{format.HDR_BATCH_SOURCE_NAME}|" +
                        $"{format.HDR_GL_DATE}|" +
                        $"{format.HDR_SOURCE_REFERENCE}|" +
                        $"{format.DTL_LINE_DESC}|" +
                        $"{format.DTL_QUANTITY}|" +
                        $"{format.DTL_AMOUNT}|" +
                        $"{format.DTL_VAT_CODE}|" +
                        $"{format.DTL_CURRENCY}|" +
                        $"{format.INVOICE_APPLIED}|" +
                        $"{filename}|";
                    content.AppendLine(line);
                    var filePath = Path.Combine(custTranList.FilePath, filename);
                    await File.AppendAllTextAsync(filePath, line + Environment.NewLine);

                    var formattedResult = i.CustomerCode;
                    var customerName = string.Empty;
                    if (formattedResult != null)
                    {
                        customerName = custCodeList
                            .Where(cc => cc.CustomerCode == formattedResult)
                            .Select(cc => cc.CustomerName)
                            .FirstOrDefault();
                    }

                    var generateInvoice = new GenerateInvoiceDto
                    {
                        Club = i.Location,
                        CustomerCode = i.CustomerCode,
                        CustomerNo = getCustomerNo.CustomerNo,
                        CustomerName = customerName,
                        InvoiceNo = i.CMInvoice,
                        InvoiceDate = DateTime.Parse(custTranList?.SelectedDate),
                        TransactionDate = DateTime.Parse(custTranList.SelectedDate),
                        Location = getShortName.ShortName,
                        ReferenceNo = i.JobOrderNo.Replace("-", ""),
                        InvoiceAmount = i.TotalAmount,
                        FileName = invoiceAnalytics.FirstOrDefault().FILENAME,
                        Remarks = "CREDIT MEMO INVOICE",
                    };

                    var genInvoice = _mapper.Map<GenerateInvoiceDto, GenerateInvoice>(generateInvoice);
                    _dbContext.GenerateInvoice.Add(genInvoice);
                    await _dbContext.SaveChangesAsync();

                    //var param1 = new GenerateA0FileDto
                    //{
                    //    Path = "",
                    //    analyticsParamsDto = new AnalyticsParamsDto
                    //    {
                    //        dates = new List<string> { custTranList.SelectedDate.ToString() },
                    //        memCode = new List<string> { i.CustomerCode.ToString() },
                    //        storeId = new List<int> { i.Location },
                    //        orderNo = i.JobOrderNo.ToString(),
                    //    }
                    //};

                    result = true;
                }
            }
            catch (Exception ex)
            {
                throw;
            }

            return result;
        }
        #endregion

        #region Reload
        public async Task<CreditMemoTranDto> RetrieveUpdateCreditMemoData(VarianceParams variance)
        {
            try
            {
                //var dateFrom = DateTime.Now.AddDays(-1);
                var results = new List<CMTranDto>();
                var formattedDate = decimal.Parse(!string.IsNullOrEmpty(variance.CurrentDate) ? variance.CurrentDate : "0");

                var result = await _dbContext.TempViewCMMMS.FromSqlRaw($@"SELECT A.CSDATE, A.CSSTOR, A.CSREG, A.CSCUST, A.CSTAMT,B.CSTDOC,B.CSTRAN,B.CSCARD,B.CSDTYP,B.CSTIL,B.CSSEQ " +
                    $@"FROM OPENQUERY([{_linkedServerOptions.MMS}], 'SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSCUST, CSTAMT FROM MMJDALIB.CSHHDR WHERE CSDATE = ''{formattedDate}''') A " +
                    $@"INNER JOIN (SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL, CSSEQ " +
                    $@"FROM OPENQUERY([{_linkedServerOptions.MMS}], 'SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL, CSSEQ FROM MMJDALIB.CSHTND WHERE CSDATE = {formattedDate} " +
                    $@"AND CSDTYP = ''CM'' AND CSSTOR = {variance.Store} GROUP BY CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL, CSSEQ ')) B " +
                    $@"ON A.CSDATE = B.CSDATE AND A.CSSTOR = B.CSSTOR AND A.CSREG = B.CSREG AND A.CSTRAN = B.CSTRAN").ToListAsync();


                var tempModel = result.Select(n => new CMTranDto
                {

                    CSDATE = n.CSDATE,
                    CSSTOR = n.CSSTOR,
                    CSREG = n.CSREG,
                    CSTRAN = n.CSTRAN,
                    CSCUST = n.CSCUST,
                    CSTAMT = n.CSTAMT,
                    CSTDOC = n.CSTDOC,
                    CSCARD = n.CSCARD,
                    CSDTYP = n.CSDTYP,
                    CSTIL = n.CSTIL,
                    CSSEQ = n.CSSEQ
                }).AsQueryable();

                foreach (var item in tempModel)
                {
                    var cmdet = await _dbContext.CMTransaction.Where(x => x.Location == (int)item.CSSTOR && x.TransactionDate == item.CSDATE).FirstOrDefaultAsync();

                    if (cmdet != null)
                    {
                        _dbContext.CMTransaction.Remove(cmdet);
                        await _dbContext.SaveChangesAsync();
                    }
                    var model = new CMTransaction
                    {
                        CustomerCode = string.IsNullOrEmpty(item.CSTDOC) ? string.Empty : item.CSTDOC,
                        Location = (int)item.CSSTOR,
                        TransactionDate = item.CSDATE,
                        MembershipNo = item.CSCUST.ToString(),
                        CashierNo = item.CSTIL.ToString(),
                        RegisterNo = item.CSREG.ToString(),
                        TrxNo = item.CSTRAN.ToString(),
                        JobOrderNo = string.IsNullOrEmpty(item.CSCARD) ? string.Empty : item.CSCARD,
                        Amount = item.CSTAMT,
                        Status = string.IsNullOrEmpty(item.CSTDOC) || string.IsNullOrEmpty(item.CSCARD) ? (int)StatusEnums.EXCEPTION : (int)StatusEnums.PENDING,
                        ModifiedDate = DateTime.Now,
                        ModifiedBy = "System",
                        IsDeleted = false,
                        Seq = (long)item.CSSEQ
                    };

                    _dbContext.CMTransaction.Add(model);
                    await _dbContext.SaveChangesAsync();
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            return await RetriveCreditMemo(variance);
        }
        #endregion

        #region Credit Memo Invoice Report
        public async Task<List<GenerateInvoice>> GetCreditMemoInvoice(CreditMemoInvoiceDto req)
        {
            var generateInvList = new List<GenerateInvoice>();
            try
            {
                var dates = new List<string>();
                var stores = new List<int>();
                var merchants = new List<string>();
                stores.AddRange(req.StoreId);
                merchants.AddRange(req.MerchantCode);
                foreach (var date in req.Dates)
                {
                    DateTime.TryParse(date, out DateTime selectedDate);
                    dates.Add(selectedDate.ToString("yyMMdd"));
                }

                var getCMTranDateRange = await _dbContext.VW_CMTransactions.Where(x => (x.TransactionDate == decimal.Parse(dates[0]) || x.TransactionDate == decimal.Parse(dates[1]))
                    && stores.Contains(x.Location) && merchants.Contains(x.CustomerCode)).Distinct().ToListAsync();
                foreach (var item in getCMTranDateRange)
                {
                    var getCustNo = await _dbContext.CustomerCodes.Where(x => x.CustomerCode == item.CustomerCode).Select(x => x.CustomerNo).FirstOrDefaultAsync();
                    DateTime.TryParseExact(item.TransactionDate.ToString("0"), "yyMMdd", System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out DateTime transactionDate);
                    var genInvItem = new GenerateInvoice();
                    genInvItem.Id = (int)item.Id;
                    genInvItem.Club = item.Location;
                    genInvItem.CustomerNo = item.CustomerCode;
                    genInvItem.CustomerCode = item.CustomerCode;
                    genInvItem.CustomerName = item.CustomerName;
                    genInvItem.InvoiceNo = item.CMInvoiceNo;
                    genInvItem.InvoiceDate = transactionDate;
                    genInvItem.TransactionDate = transactionDate;
                    genInvItem.Location = item.Location.ToString();
                    genInvItem.ReferenceNo = item.ReferenceNo;
                    genInvItem.InvoiceAmount = item.Amount;
                    genInvItem.FileName = item.FileName;
                    generateInvList.Add(genInvItem);
                }
            }
            catch (Exception ex)
            {
                throw;
            }
            return generateInvList;
        }
        #endregion

        #region Private Functions
        private async Task<bool> UpdateCreditMemoMMS(CustomerTransactionDto request)
        {
            var result = false;
            try
            {
                await _dbContext.Database.ExecuteSqlRawAsync($@"EXEC('UPDATE MMJDALIB.CSHTND SET CSCARD = ''{request?.JobOrderNo}'', CSTDOC = ''{request?.CustomerCode}''" +
                $"WHERE CSSTOR = {request?.Club} AND CSDATE = ''{request?.TransactionDate}'' AND CSREG = ''{request?.RegisterNo}'' AND CSTRAN = ''{request?.TransactionNo}'' " +
                $"AND CSDTYP = ''CM'' AND CSSEQ = {request?.Seq}') AT [{_linkedServerOptions.MMS}]");
                result = true;
            }
            catch (Exception ex)
            {
                //Log errors
                var message = ex.Message;
                throw;
            }
            return result;
        }

        private async Task<CreditMemoTranDto> RetriveCreditMemo(VarianceParams variance)
        {
            try
            {
                var result = new CreditMemoTranDto();
                var formattedDate = decimal.Parse(!string.IsNullOrEmpty(variance.CurrentDate) ? variance.CurrentDate : "0.00");
                var csi = await _dbContext.CMTransaction.Where(x => x.TransactionDate == formattedDate && x.Location == variance.Store).Select(x => x.Amount).SumAsync();
                var mms = _dbContext.TempVwMMSDto.FromSqlRaw($"SELECT * FROM OPENQUERY([{_linkedServerOptions.MMS}]," +
                    $"'select * from mmjdalib.cshrep where csstor = {variance.Store} " +
                    $"and csdate = {formattedDate} and cstlin = 723 and csreg > 0 and cstil = 0 order by csrpam')").FirstOrDefault();

                if (mms == null || csi == null)
                {
                    result = new CreditMemoTranDto
                    {
                        Variance = new VarianceMMS
                        {
                            MMS = 0,
                            CSI = 0,
                            Variance = 0
                        },
                        CMTranList = new List<CustomerTransactionDto>()
                    };
                }
                else
                {
                    var custTranDto = new List<CustomerTransactionDto>();
                    if (string.IsNullOrEmpty(variance.searchQuery))
                    {
                        var vw_CmTran = await _dbContext.VW_CMTransactions.Where(x => x.TransactionDate == formattedDate && x.Location == variance.Store).ToListAsync();
                        foreach (var a in vw_CmTran)
                        {
                            var custDto = new CustomerTransactionDto();
                            custDto.Id = (int?)a.Id;
                            custDto.CustomerCode = a.CustomerCode;
                            custDto.CustomerName = a.CustomerName;
                            custDto.TransactionDate = a.TransactionDate.ToString();
                            custDto.MembershipNo = a.MembershipNo;
                            custDto.CashierNo = a.CashierNo;
                            custDto.RegisterNo = a.RegisterNo;
                            custDto.TransactionNo = a.TransactionNo;
                            custDto.JobOrderNo = a.JobOrderNo;
                            custDto.Amount = a.Amount;
                            custDto.Status = a.Status;
                            custDto.IsDeleted = a.IsDeleted;
                            custDto.Seq = a.Seq;
                            custDto.Club = a.Location;
                            custTranDto.Add(custDto);
                        }
                    }
                    else
                    {
                        var vw_CmTran = await _dbContext.VW_CMTransactions.Where(x => x.TransactionDate == formattedDate && x.Location == variance.Store &&
                        (x.CustomerName.Contains(variance.searchQuery) || x.JobOrderNo == variance.searchQuery || x.CustomerCode == variance.searchQuery)).ToListAsync();
                        foreach (var a in vw_CmTran)
                        {
                            var custDto = new CustomerTransactionDto();
                            custDto.Id = (int?)a.Id;
                            custDto.CustomerCode = a.CustomerCode;
                            custDto.CustomerName = a.CustomerName;
                            custDto.TransactionDate = a.TransactionDate.ToString();
                            custDto.MembershipNo = a.MembershipNo;
                            custDto.CashierNo = a.CashierNo;
                            custDto.RegisterNo = a.RegisterNo;
                            custDto.TransactionNo = a.TransactionNo;
                            custDto.JobOrderNo = a.JobOrderNo;
                            custDto.Amount = a.Amount;
                            custDto.Status = a.Status;
                            custDto.IsDeleted = a.IsDeleted;
                            custDto.Seq = a.Seq;
                            custDto.Club = a.Location;
                            custTranDto.Add(custDto);
                        }
                    }

                    result = new CreditMemoTranDto
                    {
                        Variance = new VarianceMMS
                        {
                            MMS = mms.CSRPAM,
                            CSI = csi,
                            Variance = mms.CSRPAM - csi,
                        },
                        CMTranList = custTranDto,
                    };
                }
                return result;
            }
            catch (Exception ex)
            {
                throw;
            }
        } 
        #endregion
    }
}
