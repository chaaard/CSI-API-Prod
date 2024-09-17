using AutoMapper;
using Azure.Core;
using CSI.Application.DTOs;
using CSI.Application.Enums;
using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
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


        public CreditMemoService(AppDBContext dbContext, IConfiguration configuration, IMapper mapper, IDbContextFactory<AppDBContext> contextFactory,
             IOptions<LinkedServerOptions> linkedServerOptions, IAnalyticsService analyticsService)
        {
            _dbContext = dbContext;
            _configuration = configuration;
            _mapper = mapper;
            _contextFactory = contextFactory;
            _linkedServerOptions = linkedServerOptions.Value;
            _analyticsService = analyticsService;
        }

        public async Task<CreditMemoTranDto> GetCMVariance(VarianceParams variance)
        {
            return await RetriveCreditMemo(variance);
        }

        public async Task<bool> UpdateCustCreditMemo(CustomerTransactionDto custDto) // Update Status to Pending
        {
            bool result = false;
            var locateId = await _dbContext.CMTransaction.Where(x => x.Id == custDto.Id).FirstOrDefaultAsync();
            if (locateId != null)
            {
                locateId.CustomerCode = custDto.CustomerCode ?? string.Empty;
                locateId.JobOrderNo = custDto?.JobOrderNo ?? string.Empty;
                locateId.Status = (int)StatusEnums.PENDING;
                locateId.ModifiedBy = custDto?.ModifiedBy;
                locateId.ModifiedDate = DateTime.Now;

                await _dbContext.SaveChangesAsync();
                result = true;
            }

            //Updates the MMS
            UpdateCreditMemoMMS(custDto);

            return result;
        }
        public bool UpdateCreditMemoStatus(CreditMemoDto custTranList) //Submit all
        {
            var result = false;
            var results = new List<AccntGenerateInvoiceDto>();
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            var filename = "CM" + DateTime.Now.ToString("MMddyy_hhmmss") + ".A01";
            //loop
            foreach (var item in custTranList.CMTranList)
            {
                var custId = _dbContext.CMTransaction.Where(x => x.Id == item.Id).FirstOrDefault();
                if (custId != null)
                {

                    results.Add(GenerateCMInvoice(item,custTranList.SelectedDate));
                    var action = "Refresh Generate Invoice";
                    var remarks = "Successfully Refreshed";
                    Logger(custTranList.Id, action, remarks, item.Club.ToString(), custId.CustomerCode);
                    //custId.Status = (int)StatusEnums.SUBMITTED;
                    //_dbContext.SaveChanges();
                    //result = true;
                }
            }
            if (results.Count() >= 1)
            {
                var submittedInvoice = results.Where(x => x.SubmitStatus == (int)StatusEnums.PENDING 
                && x.IsGenerated == false).ToList();

                if (submittedInvoice.Count() == 0)
                {
                    var action = "Generate A01 Credit Memo Invoice";
                    var remarks = "Error: Error generating credit memo invoice. Please check and try again.";
                    Logger(custTranList.Id, action, remarks);
                }
                else
                {
                    //actual generation.
                    foreach (var i in submittedInvoice)
                    {
                        var cmInvoiceList = new List<InvoiceDto>();
                        GenerateA0FilePerMerchant(i);
                        //var total = analyticsResult?.Result.Sum(x => x.SubTotal);
                    }
                }
            }
            else
            {
                var action = "Generate A01 Credit Memo Invoice";
                var remarks = "Error: Error generating credit memo invoice. Please check and try again.";
                Logger(custTranList.Id, action, remarks);
            }
            return result;
        }

        public async Task<CreditMemoTranDto> RetrieveUpdateCreditMemoData(VarianceParams variance)
        {
            //var dateFrom = DateTime.Now.AddDays(-1);
            var results = new List<CMTranDto>();
            var formattedDate = decimal.Parse(!string.IsNullOrEmpty(variance.CurrentDate) ? variance.CurrentDate : "0");

            var result = await _dbContext.TempDto.FromSqlRaw($@"SELECT A.CSDATE, A.CSSTOR, A.CSREG, A.CSCUST, A.CSTAMT,B.CSTDOC,B.CSTRAN,B.CSCARD,B.CSDTYP,B.CSTIL,B.CSSEQ " +
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
                CSTIL =  n.CSTIL,
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
                    CashierNo = string.Empty,
                    TrxNo = item.CSTRAN.ToString(),
                    JobOrderNo = string.IsNullOrEmpty(item.CSCARD) ? string.Empty : item.CSCARD,
                    Amount = item.CSTAMT,
                    Status = string.IsNullOrEmpty(item.CSTDOC) && string.IsNullOrEmpty(item.CSCARD) ? (int)StatusEnums.EXCEPTION : (int)StatusEnums.PENDING,
                    ModifiedDate = DateTime.Now,
                    ModifiedBy = "System",
                    IsDeleted = false,
                    Seq = (long)item.CSSEQ
                };

                _dbContext.CMTransaction.Add(model);
                await _dbContext.SaveChangesAsync();
            }

            return await RetriveCreditMemo(variance);
        }

        public async Task<CreditMemoTranDto> SearchCreditMemoItem(CMSearchParams searchParams)
        {
            var result = new CreditMemoTranDto();
            var dateFrom = DateTime.Now.AddDays(-1);
            var formattedDate = dateFrom.ToString("yyMMdd");
            var vwcm = await _dbContext.VW_CMTransactions.Where(x => x.TransactionDate == decimal.Parse(formattedDate) && x.Location == searchParams.LocationId &&
                x.CustomerCode == searchParams.CustomerCode || x.JobOrderNo == searchParams.JobOrderNo).ToListAsync();
            var custTranDto = new List<CustomerTransactionDto>();
            var varData = new VarianceMMS
            {
                MMS = searchParams.Variance.MMS,
                CSI = searchParams.Variance.CSI,
                Variance = searchParams.Variance.Variance
            };
            foreach (var i in vwcm)
            {
                var custDto = new CustomerTransactionDto();
                custDto.Id = (int?)i.Id;
                custDto.CustomerCode = i.CustomerCode;
                custDto.CustomerName = i.CustomerName;
                custDto.TransactionDate = i.TransactionDate.ToString();
                custDto.MembershipNo = i.MembershipNo;
                custDto.CashierNo = i.CashierNo;
                custDto.RegisterNo = i.RegisterNo;
                custDto.TransactionNo = i.TransactionNo;
                custDto.JobOrderNo = i.JobOrderNo;
                custDto.Amount = i.Amount;
                custDto.Status = i.Status;
                custDto.IsDeleted = i.IsDeleted;
                custDto.Seq = i.Seq;
                custTranDto.Add(custDto);
            }
            return result;
        }

        private void UpdateCreditMemoMMS(CustomerTransactionDto request)
        {
            _dbContext.Database.ExecuteSqlRawAsync($@"EXEC('UPDATE MMJDALIB.CSHTND SET CSCARD = ''{request?.JobOrderNo}'', CSTDOC = ''{request?.CustomerCode}''" +
                $"WHERE CSSTOR = ''{request?.Club}'' AND CSDATE = ''{request?.TransactionDate}'' AND CSREG = ''{request?.RegisterNo}'' AND CSTRAN = ''{request?.TransactionNo}'' " +
                $"AND CSDTYP = ''CM'' AND CSSEQ = {request?.Seq}') AT [{_linkedServerOptions.MMS}]");
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
                        (x.CustomerName == variance.searchQuery || x.JobOrderNo == variance.searchQuery)).ToListAsync();
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
        private AccntGenerateInvoiceDto GenerateCMInvoice(CustomerTransactionDto request,string selectedDate)
        {
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            var result = new AccntGenerateInvoiceDto();
            try
            {
                //string[] orderNoList = { "CSI", "PV" };
                //var locateStore = _dbContext.Locations.Where(loc => loc.LocationCode == request.Club).FirstOrDefault();
                //var locateAnalytics = _dbContext.Analytics.Where(x => x.TransactionDate.ToString() == selectedDate &&
                //       x.DeleteFlag == false && x.CustomerId == request.CustomerCode && x.OrderNo == request.JobOrderNo && x.TransactionNo == request.TransactionNo).FirstOrDefault();
                //if (locateAnalytics != null)
                //{
                //    result.Id = locateAnalytics.Id;
                //    result.CustomerId = locateAnalytics.CustomerId;
                //    result.Date = DateTime.Parse(selectedDate);
                //    result.Location = locateStore.LocationName;
                //    result.LocationId = locateStore.LocationCode;
                //    result.SubmitStatus = locateAnalytics != null ? locateAnalytics.StatusId : 0;
                //    result.IsGenerated = locateAnalytics.IsGenerate;
                //}
                var getCMtransaction = _dbContext.CMTransaction.Where(x => x.Id == request.Id).FirstOrDefault();
                if (getCMtransaction != null)
                {
                    result.Id = (int)getCMtransaction.Id;
                    result.CustomerId = getCMtransaction.CustomerCode;
                    result.Date = DateTime.Parse(selectedDate);
                    result.Location = getCMtransaction.Location.ToString();
                    result.SubmitStatus = getCMtransaction.Status;
                    result.IsGenerated = getCMtransaction.Status == (int)StatusEnums.PENDING ? true : false;
                }
            }
            catch (Exception ex) 
            {
                throw ex;
            }
            return result;
        }
        private void Logger(string userId,string action,string remarks, string? club = null,string? customerId = null)
        {
            var logDto = new LogsDto();
            var logsMap = new Logs();
            logDto.UserId = userId;
            logDto.Date = DateTime.Now;
            logDto.Action = action;
            logDto.Remarks = remarks;
            logDto.Club = string.IsNullOrEmpty(club) ? "0":club;
            logDto.CustomerId = string.IsNullOrEmpty(customerId) ? string.Empty:customerId;
            logsMap = _mapper.Map<LogsDto, Logs>(logDto);
            _dbContext.Logs.Add(logsMap);
            _dbContext.SaveChanges();
        }

        private async void GenerateA0FilePerMerchant(AccntGenerateInvoiceDto accInv)
        {
            DateTime givenDt = (DateTime)(accInv.Date);
            var formattedDate = givenDt.ToString("MMddyy");
            var locationList = await _analyticsService.GetLocations();
            var locateCm = _dbContext.CMTransaction.Where(c => c.TransactionDate == decimal.Parse(formattedDate)
                && c.CustomerCode == accInv.CustomerId && c.Location == accInv.LocationId).ToList();
            var total = locateCm.Sum(s => s.Amount);
            var trxCnt = locateCm.Count();

            var lastCmInvoice = await _dbContext.CMTransaction.OrderByDescending(i => i.Id).FirstOrDefaultAsync();
            long startingCmInvoiceNo = 000000000001;
            if (lastCmInvoice.CMInvoiceNo != null)
                startingCmInvoiceNo = startingCmInvoiceNo + 1;
            long newCmInvoiceNo = startingCmInvoiceNo;

            while(await _dbContext.CMTransaction.AnyAsync(i => i.CMInvoiceNo.Substring(2,i.CMInvoiceNo.Length) == newCmInvoiceNo.ToString("000000000000")))
            {
                newCmInvoiceNo++;
            }
            var formattedCmInvNo = newCmInvoiceNo.ToString("000000000000");
            var convLocation = int.Parse(accInv.Location);
            var getShortName = locationList.Where(x => x.LocationCode == convLocation).Select(a => new
            {
                a.ShortName,
            }).FirstOrDefault();
            var getCustomerNo = locateCm.GroupJoin(_dbContext.CustomerCodes, x => x.CustomerCode, y =>
                y.CustomerCode, (x, y) => new { x, y }).SelectMany(group => group.y, (group, y) => y.CustomerNo).FirstOrDefault();
            var formatCustomerNo = getCustomerNo.Replace("P", "").Trim();
            var getReference = await _dbContext.Reference.Where(x => x.CustomerNo == formatCustomerNo).Select(n => new
            {
                n.MerchReference
            }).FirstOrDefaultAsync();

        }
    }
}
