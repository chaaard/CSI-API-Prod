using AutoMapper;
using AutoMapper.Configuration.Annotations;
using AutoMapper.Execution;
using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using EFCore.BulkExtensions;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualBasic;
using OfficeOpenXml.Style;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;

namespace CSI.Application.Services
{
    public class AnalyticsService : IAnalyticsService
    {
        private readonly AppDBContext _dbContext;
        private readonly IConfiguration _configuration;
        private readonly IMapper _mapper;
        private readonly IDbContextFactory<AppDBContext> _contextFactory;
        private readonly IAdjustmentService _adjustmentService;

        public AnalyticsService(IConfiguration configuration, AppDBContext dBContext, IMapper mapper, IDbContextFactory<AppDBContext> contextFactory, IAdjustmentService adjustmentService)
        {
            _configuration = configuration;
            _dbContext = dBContext;
            _mapper = mapper;
            _dbContext.Database.SetCommandTimeout(999);
            _contextFactory = contextFactory;
            _adjustmentService = 
            _adjustmentService = adjustmentService;
        }

        public async Task<string> GetDepartments()
        {
            try
            {
                List<string> values = new List<string>();
                using (MsSqlCon db = new MsSqlCon(_configuration))
                {
                    if (db.Con.State == ConnectionState.Closed)
                    {
                        await db.Con.OpenAsync();
                    }

                    var cmd = new SqlCommand();
                    cmd.Connection = db.Con;
                    cmd.CommandType = CommandType.Text;
                    cmd.CommandTimeout = 0;
                    cmd.CommandText = "SELECT DeptCode FROM TrsDept_Table";
                    cmd.ExecuteNonQuery();
                    SqlDataReader sqlDataReader = cmd.ExecuteReader();

                    if (sqlDataReader.HasRows)
                    {
                        while (sqlDataReader.Read())
                        {
                            if (sqlDataReader["DeptCode"].ToString() != null)
                                values.Add(sqlDataReader["DeptCode"].ToString());
                        }
                    }
                    sqlDataReader.Close();
                    await db.Con.CloseAsync();
                }

                return string.Join(", ", (IEnumerable<string>)values); ;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async Task<List<Location>> GetLocations()
        {
            try
            {
                var locations = new List<Location>();
                locations = await _dbContext.Locations
                    .ToListAsync();

                return locations;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async Task<List<AnalyticsDto>> GetAnalytics(AnalyticsParamsDto analyticsParamsDto)
        {
            try
            {
                var analytics = await ReturnAnalytics(analyticsParamsDto);

                return analytics;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private async Task<List<AnalyticsDto>> ReturnAnalytics(AnalyticsParamsDto analyticsParamsDto)
        {
            List<string> memCodeLast6Digits = analyticsParamsDto.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
            var analyticsList = new List<AnalyticsDto>();
            DateTime date;
            var analytics = new List<AnalyticsDto>();
            if (DateTime.TryParse(analyticsParamsDto.dates[0].ToString(), out date))
            {
               
                var result = await _dbContext.AnalyticsView
              .FromSqlRaw($" SELECT  " +
                          $"     MAX(a.Id) AS Id, " +
                          $"     MAX(a.CustomerId) AS CustomerId, " +
                          $"     MAX(a.CustomerName) AS CustomerName, " +
                          $"     MAX(a.LocationId) AS LocationId, " +
                          $"     MAX(a.LocationName) AS LocationName, " +
                          $"     MAX(a.TransactionDate) AS TransactionDate, " +
                          $"     MAX(a.MembershipNo) AS MembershipNo, " +
                          $"     MAX(a.CashierNo) AS CashierNo, " +
                          $"     MAX(a.RegisterNo) AS RegisterNo, " +
                          $"     MAX(a.TransactionNo) AS TransactionNo, " +
                          $"     a.OrderNo, " +
                          $"     MAX(a.Qty) AS Qty, " +
                          $"     MAX(a.Amount) AS Amount, " +
                          $"     MAX(CAST(a.StatusId AS INT)) AS StatusId,  " +
                          $"     MAX(CAST(a.DeleteFlag AS INT)) AS DeleteFlag, " +
                          $"     MAX(CAST(a.IsUpload AS INT)) AS IsUpload, " +
                          $"     MAX(CAST(a.IsGenerate AS INT)) AS IsGenerate, " +
                          $"     MAX(CAST(a.IsTransfer AS INT)) AS IsTransfer, " +
                          $"     MAX(a.SubTotal) AS SubTotal  " +
                          $" FROM ( " +
                          $"     SELECT   " +
                          $"         n.Id, " +
                          $"         n.CustomerId,  " +
                          $"         c.CustomerName,  " +
                          $"         n.LocationId,  " +
                          $"         l.LocationName,  " +
                          $"         n.TransactionDate,   " +
                          $"         n.MembershipNo,   " +
                          $"         n.CashierNo,  " +
                          $"         n.RegisterNo,  " +
                          $"         n.TransactionNo,  " +
                          $"         n.OrderNo,  " +
                          $"         n.Qty,  " +
                          $"         n.Amount,  " +
                          $"         n.SubTotal, " +
                          $"         n.StatusId, " +
                          $"         n.DeleteFlag,   " +
                          $"         n.IsUpload,   " +
                          $"         n.IsGenerate,   " +
                          $"         n.IsTransfer,   " +
                          $"         ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                          $"     FROM tbl_analytics n " +
                          $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                          $"        INNER JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                          $"     WHERE  " +
                          $"     (CAST(TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND LocationId = {analyticsParamsDto.storeId[0]} AND n.DeleteFlag = 0) " +
                          $"         AND ({string.Join(" OR ", analyticsParamsDto.memCode.Select(code => $"CustomerId LIKE '%{code.Substring(Math.Max(0, code.Length - 6))}%'"))}) " +
                          $" ) a " +
                          $" GROUP BY  " +
                          $"     a.OrderNo,    " +
                          $"     ABS(a.SubTotal),  " +
                          $"     a.row_num " +
                          $" HAVING " +
                          $"     COUNT(a.OrderNo) = 1 "
                          )
                 .ToListAsync();
                analytics = result.Select(n => new AnalyticsDto
                {
                    Id = n.Id,
                    CustomerId = n.CustomerId,
                    CustomerName = n.CustomerName,
                    LocationName = n.LocationName,
                    TransactionDate = n.TransactionDate,
                    MembershipNo = n.MembershipNo,
                    CashierNo = n.CashierNo,
                    RegisterNo = n.RegisterNo,
                    TransactionNo = n.TransactionNo,
                    OrderNo = n.OrderNo,
                    Qty = n.Qty,
                    Amount = n.Amount,
                    SubTotal = n.SubTotal,
                    StatusId = n.StatusId,
                    IsUpload = Convert.ToBoolean(n.IsUpload),
                    IsGenerate = Convert.ToBoolean(n.IsGenerate),
                    IsTransfer = Convert.ToBoolean(n.IsTransfer),
                    DeleteFlag = Convert.ToBoolean(n.DeleteFlag),
                }).ToList();
            }

            return analytics;

        }

        private async Task<List<Analytics>> GetRawAnalytics(AnalyticsParamsDto analyticsParamsDto)
        {
            List<string> memCodeLast6Digits = analyticsParamsDto.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
            DateTime date;
            var analytics = new List<Analytics>();
            if (DateTime.TryParse(analyticsParamsDto.dates[0].ToString(), out date))
            {
                var result = await _dbContext.AnalyticsView
              .FromSqlRaw($" SELECT  " +
                          $"     MAX(a.Id) AS Id, " +
                          $"     MAX(a.CustomerId) AS CustomerId, " +
                          $"     MAX(a.CustomerName) AS CustomerName, " +
                          $"     MAX(a.LocationId) AS LocationId, " +
                          $"     MAX(a.LocationName) AS LocationName, " +
                          $"     MAX(a.TransactionDate) AS TransactionDate, " +
                          $"     MAX(a.MembershipNo) AS MembershipNo, " +
                          $"     MAX(a.CashierNo) AS CashierNo, " +
                          $"     MAX(a.RegisterNo) AS RegisterNo, " +
                          $"     MAX(a.TransactionNo) AS TransactionNo, " +
                          $"     a.OrderNo, " +
                          $"     MAX(a.Qty) AS Qty, " +
                          $"     MAX(a.Amount) AS Amount, " +
                          $"     MAX(CAST(a.StatusId AS INT)) AS StatusId,  " +
                          $"     MAX(CAST(a.DeleteFlag AS INT)) AS DeleteFlag, " +
                          $"     MAX(CAST(a.IsUpload AS INT)) AS IsUpload, " +
                          $"     MAX(CAST(a.IsGenerate AS INT)) AS IsGenerate, " +
                          $"     MAX(CAST(a.IsTransfer AS INT)) AS IsTransfer, " +
                          $"     MAX(a.SubTotal) AS SubTotal  " +
                          $" FROM ( " +
                          $"     SELECT   " +
                          $"         n.Id, " +
                          $"         n.CustomerId,  " +
                          $"         c.CustomerName,  " +
                          $"         n.LocationId,  " +
                          $"         l.LocationName,  " +
                          $"         n.TransactionDate,   " +
                          $"         n.MembershipNo,   " +
                          $"         n.CashierNo,  " +
                          $"         n.RegisterNo,  " +
                          $"         n.TransactionNo,  " +
                          $"         n.OrderNo,  " +
                          $"         n.Qty,  " +
                          $"         n.Amount,  " +
                          $"         n.SubTotal, " +
                          $"         n.StatusId, " +
                          $"         n.DeleteFlag,   " +
                          $"         n.IsUpload,   " +
                          $"         n.IsGenerate,   " +
                          $"         n.IsTransfer,   " +
                          $"         ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                          $"     FROM tbl_analytics n " +
                          $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                          $"        INNER JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                          $"     WHERE  " +
                          $"        (CAST(TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND LocationId = {analyticsParamsDto.storeId[0]} AND CustomerId LIKE '%{memCodeLast6Digits[0]}%' AND n.DeleteFlag = 0) " +
                          $" ) a " +
                          $" GROUP BY  " +
                          $"     a.OrderNo,    " +
                          $"     ABS(a.SubTotal),  " +
                          $"     a.row_num " +
                          $" HAVING " +
                          $"     COUNT(a.OrderNo) = 1 "
                          )
                 .ToListAsync();

                analytics = result.Select(n => new Analytics
                {
                    Id = n.Id,
                    CustomerId = n.CustomerId,
                    LocationId = n.LocationId,
                    TransactionDate = n.TransactionDate,
                    MembershipNo = n.MembershipNo,
                    CashierNo = n.CashierNo,
                    RegisterNo = n.RegisterNo,
                    TransactionNo = n.TransactionNo,
                    OrderNo = n.OrderNo,
                    Qty = n.Qty,
                    Amount = n.Amount,
                    SubTotal = n.SubTotal,
                    StatusId = n.StatusId,
                    IsUpload = Convert.ToBoolean(n.IsUpload),
                    IsGenerate = Convert.ToBoolean(n.IsGenerate),
                    IsTransfer = Convert.ToBoolean(n.IsTransfer),
                    DeleteFlag = Convert.ToBoolean(n.DeleteFlag),
                }).ToList();
            }

            return analytics;

        }

        private async Task<List<AnalyticsDto>> ReturnAnalyticsSubmit(RefreshAnalyticsDto refreshAnalyticsDto)
        {
            List<string> memCodeLast6Digits = refreshAnalyticsDto.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
            DateTime date;
            var analytics = new List<AnalyticsDto>();
            if (DateTime.TryParse(refreshAnalyticsDto.dates[0].ToString(), out date))
            {
                var result = await _dbContext.AnalyticsView
                .FromSqlRaw($" SELECT  " +
                         $"     MAX(a.Id) AS Id, " +
                         $"     MAX(a.CustomerId) AS CustomerId, " +
                         $"     MAX(a.CustomerName) AS CustomerName, " +
                         $"     MAX(a.LocationId) AS LocationId, " +
                         $"     MAX(a.LocationName) AS LocationName, " +
                         $"     MAX(a.TransactionDate) AS TransactionDate, " +
                         $"     MAX(a.MembershipNo) AS MembershipNo, " +
                         $"     MAX(a.CashierNo) AS CashierNo, " +
                         $"     MAX(a.RegisterNo) AS RegisterNo, " +
                         $"     MAX(a.TransactionNo) AS TransactionNo, " +
                         $"     a.OrderNo, " +
                         $"     MAX(a.Qty) AS Qty, " +
                         $"     MAX(a.Amount) AS Amount, " +
                         $"     MAX(CAST(a.StatusId AS INT)) AS StatusId,  " +
                         $"     MAX(CAST(a.DeleteFlag AS INT)) AS DeleteFlag, " +
                         $"     MAX(CAST(a.IsUpload AS INT)) AS IsUpload, " +
                         $"     MAX(CAST(a.IsGenerate AS INT)) AS IsGenerate, " +
                         $"     MAX(CAST(a.IsTransfer AS INT)) AS IsTransfer, " +
                         $"     MAX(a.SubTotal) AS SubTotal  " +
                         $" FROM ( " +
                         $"     SELECT   " +
                         $"         n.Id, " +
                         $"         n.CustomerId,  " +
                         $"         c.CustomerName,  " +
                         $"         n.LocationId,  " +
                         $"         l.LocationName,  " +
                         $"         n.TransactionDate,   " +
                         $"         n.MembershipNo,   " +
                         $"         n.CashierNo,  " +
                         $"         n.RegisterNo,  " +
                         $"         n.TransactionNo,  " +
                         $"         n.OrderNo,  " +
                         $"         n.Qty,  " +
                         $"         n.Amount,  " +
                         $"         n.SubTotal, " +
                         $"         n.StatusId, " +
                         $"         n.DeleteFlag,   " +
                         $"         n.IsUpload,   " +
                         $"         n.IsGenerate,   " +
                         $"         n.IsTransfer,   " +
                         $"         ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                         $"     FROM tbl_analytics n " +
                         $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                         $"        INNER JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                         $"     WHERE  " +
                         $"        (CAST(TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND LocationId = {refreshAnalyticsDto.storeId[0]} AND CustomerId LIKE '%{memCodeLast6Digits[0]}%') AND n.DeleteFlag = 0" +
                         $" ) a " +
                         $" GROUP BY  " +
                         $"     a.OrderNo,    " +
                         $"     ABS(a.SubTotal),  " +
                         $"     a.row_num " +
                         $" HAVING " +
                         $"     COUNT(a.OrderNo) = 1 "
                         )
                .ToListAsync();
                analytics = result.Select(n => new AnalyticsDto
                {
                    Id = n.Id,
                    CustomerId = n.CustomerId,
                    LocationName = n.LocationName,
                    TransactionDate = n.TransactionDate,
                    MembershipNo = n.MembershipNo,
                    CashierNo = n.CashierNo,
                    RegisterNo = n.RegisterNo,
                    TransactionNo = n.TransactionNo,
                    OrderNo = n.OrderNo,
                    Qty = n.Qty,
                    Amount = n.Amount,
                    SubTotal = n.SubTotal,
                    StatusId = n.StatusId,
                    IsUpload = Convert.ToBoolean(n.IsUpload),
                    IsGenerate = Convert.ToBoolean(n.IsGenerate),
                    IsTransfer = Convert.ToBoolean(n.IsTransfer),
                    DeleteFlag = Convert.ToBoolean(n.DeleteFlag),
                }).ToList();
            }

            return analytics;
        }

        public async Task<Dictionary<string, decimal?>> GetTotalAmountPerMerchant(AnalyticsParamsDto analyticsParamsDto)
        {
            Dictionary<string, decimal?> totalAmounts = new Dictionary<string, decimal?>();
            DateTime date;
            try { 
                if (DateTime.TryParse(analyticsParamsDto.dates[0], out date))
                {
                    foreach (var memCode in analyticsParamsDto.memCode)
                    {
                        decimal? result = await _dbContext.Analytics
                            .Where(x => x.TransactionDate == date && x.LocationId == analyticsParamsDto.storeId[0] && x.CustomerId.Contains(memCode) && x.DeleteFlag == false)
                            .SumAsync(e => e.SubTotal);

                        totalAmounts.Add(memCode, result);
                    }
                }

                return totalAmounts;
            }
            catch (Exception ex)
            {
                var exs = ex.Message;
                throw;
            }

        }

        public async Task<List<AnalyticsSearchDto>> GetAnalyticsByItem(RefreshAnalyticsDto analyticsParam)
        {
            try
            {
                string strDate = analyticsParam.dates[0].ToString("yyMMdd");
                List<string> memCodeLast6Digits = analyticsParam.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
                string storeList = $"CSSTOR IN ({string.Join(", ", analyticsParam.storeId.Select(code => $"{code}"))})";
                var analytics = new List<AnalyticsSearchDto>();

                DateTime date;
                if (DateTime.TryParse(analyticsParam.dates[0].ToString(), out date))
                {
                    var result = await _dbContext.AnalyticsSearch
                                    .FromSqlRaw($@"
                                        SELECT 
                                            CAST(B.Id AS int) AS Id,
                                            CAST(C.CSSTOR AS varchar(10)) AS LocationId,  
                                            CONVERT(datetime, 
                                            CONCAT(
                                                '20', 
                                                SUBSTRING(CAST(C.CSDATE AS varchar(6)), 1, 2), 
                                                '-', 
                                                SUBSTRING(CAST(C.CSDATE AS varchar(6)), 3, 2), 
                                                '-', 
                                                SUBSTRING(CAST(C.CSDATE AS varchar(6)), 5, 2),
                                                ' 00:00:00.000'
                                            ), 
                                            120) as TransactionDate,
                                            CAST(B.CSTDOC AS varchar(20)) as CustomerId, 
				                                        D.CustomerName as CustomerName,
                                            CAST(A.CSCUST AS varchar(20)) as MembershipNo,
                                            CAST(B.CSTIL AS varchar(20)) as CashierNo, 
                                            CAST(C.CSREG AS varchar(20)) as RegisterNo, 
                                            CAST(C.CSTRAN AS varchar(20)) as TransactionNo, 
                                            CAST(B.CSCARD AS varchar(20)) as OrderNo, 
                                            CAST(SUM(C.CSQTY) AS int) AS Qty,  
                                            SUM(C.CSEXPR) AS Amount, 
                                            B.CSDAMT as SubTotal 
                                        FROM 
											(SELECT ROW_NUMBER() OVER (ORDER BY CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC) AS Id, CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSDAMT, CSTIL 
											FROM OPENQUERY(SNR, 'SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSDAMT, CSTIL
											FROM MMJDALIB.CSHTND 
											WHERE (CSDATE = {strDate}) AND CSDTYP IN (''AR'') AND {storeList} AND CSTRAN = {analyticsParam.transactionNo} AND CSREG = {analyticsParam.regNo}
											GROUP BY CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL, CSDAMT')) B  
                                        INNER JOIN 
                                            (SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSCUST, CSTAMT 
                                             FROM OPENQUERY(SNR, 
                                             'SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSCUST, CSTAMT 
                                              FROM MMJDALIB.CSHHDR 
                                              WHERE CSDATE = {strDate} AND {storeList} AND CSTRAN = {analyticsParam.transactionNo} AND CSREG = {analyticsParam.regNo}')) A
                                        ON A.CSSTOR = B.CSSTOR AND A.CSDATE = B.CSDATE AND A.CSREG = B.CSREG AND A.CSTRAN = B.CSTRAN 
                                        INNER JOIN 
                                            (SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSSKU, CSQTY, CSEXPR, CSEXCS, CSDSTS 
                                             FROM OPENQUERY(SNR, 
                                             'SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSSKU, CSQTY, CSEXPR, CSEXCS, CSDSTS 
                                              FROM MMJDALIB.CONDTX 
                                              WHERE CSDATE = {strDate} AND {storeList} AND CSSKU <> 0 AND CSDSTS = 0 AND CSTRAN = {analyticsParam.transactionNo} AND CSREG = {analyticsParam.regNo}')) C 
                                        ON A.CSSTOR = C.CSSTOR AND A.CSDATE = C.CSDATE AND A.CSREG = C.CSREG AND A.CSTRAN = C.CSTRAN
                                        INNER JOIN 
											(SELECT CustomerCode,CustomerName FROM [CSI.Development].[dbo].[tbl_customer] WHERE DeleteFlag = 0) D
										ON B.CSTDOC = D.CustomerCode 
                                        WHERE ({string.Join(" OR ", analyticsParam.memCode.Select(code => $"B.CSTDOC LIKE '%{code.Substring(Math.Max(0, code.Length - 6))}%'"))})
                                        GROUP BY 
                                            C.CSSTOR, C.CSDATE, B.CSTDOC, A.CSCUST, C.CSREG, C.CSTRAN, B.CSCARD, B.CSTIL, B.CSDAMT, D.CustomerName, B.Id
                                        ORDER BY 
                                            C.CSSTOR, C.CSDATE, C.CSREG
                                    ")
                                    .ToListAsync();
                    analytics = result.Select(n => new AnalyticsSearchDto
                    {
                        Id = n.Id,
                        CustomerId = n.CustomerId.Length > 0 ? n.CustomerId.ToString() : "",
                        CustomerName = n.CustomerName.Length > 0 ? n.CustomerName.ToString() : "",
                        LocationId = n.LocationId.Length > 0 ? n.LocationId.ToString() : "",
                        TransactionDate = n.TransactionDate,
                        MembershipNo = n.MembershipNo,
                        CashierNo = n.CashierNo,
                        RegisterNo = n.RegisterNo,
                        TransactionNo = n.TransactionNo,
                        OrderNo = n.OrderNo,
                        Qty = n.Qty,
                        Amount = n.Amount,
                        SubTotal = n.SubTotal,
                    }).ToList();
                }

                    return analytics;
            }
            catch (Exception ex)
            {
                throw;
            }
        }
        public async Task<List<MatchDto>> GetAnalyticsProofListVariance(AnalyticsParamsDto analyticsParamsDto)
        {
            try
            {
                List<string> memCodeLast6Digits = analyticsParamsDto.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
                DateTime date;
                var matchDtos = new List<MatchDto>();
                var uniqueMatches = new List<CSI.Domain.Entities.Match>();
                var duplicateMatches = new List<CSI.Domain.Entities.Match>();
                var formatDupes = new List<MatchDto>();
                var orderedResult = new List<MatchDto>();
                if (DateTime.TryParse(analyticsParamsDto.dates[0], out date))
                {
                    var result = await _dbContext.Match
                   .FromSqlRaw($"WITH RankedData AS ( " +
                               $"SELECT  " +
                               $"     MAX(a.Id) AS Id, " +
                               $"     MAX(a.LocationName) AS LocationName, " +
                               $"     MAX(a.CustomerName) AS CustomerName, " +
                               $"     MAX(a.TransactionDate) AS TransactionDate, " +
                               $"     a.OrderNo, " +
                               $"     MAX(CAST(a.IsUpload AS INT)) AS IsUpload, " +
                               $"     MAX(a.SubTotal) AS SubTotal  " +
                               $" FROM ( " +
                               $"     SELECT   " +
                               $"        n.[Id], " +
                               $"        n.LocationId, " +
                               $"        n.CustomerId, " +
                               $"        c.CustomerName, " +
                               $"        l.LocationName, " +
                               $"        n.[TransactionDate], " +
                               $"        n.[OrderNo], " +
                               $"        n.[SubTotal], " +
                               $"        n.[IsUpload],   " +
                               $"        n.[DeleteFlag],   " +
                               $"        ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                               $"     FROM tbl_analytics n " +
                               $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                               $"        INNER JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                               $"     WHERE  " +
                               $"        (CAST(TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND LocationId = {analyticsParamsDto.storeId[0]} AND CustomerId LIKE '%{memCodeLast6Digits[0]}%' AND n.DeleteFlag = 0) " +
                               $" ) a " +
                               $" GROUP BY  " +
                               $"     a.OrderNo,    " +
                               $"     ABS(a.SubTotal),  " +
                               $"     a.row_num " +
                               $" HAVING " +
                               $"     COUNT(a.OrderNo) = 1 " +
                               $"), " +
                               $"FilteredData AS ( " +
                               $"SELECT " +
                               $"    Id, " +
                               $"    CustomerName, " +
                               $"    LocationName, " +
                               $"    [TransactionDate], " +
                               $"    [OrderNo], " +
                               $"    [SubTotal], " +
                               $"    [IsUpload] " +
                               $"FROM RankedData " +
                               $") " +
                               $"SELECT " +
                               $"a.[Id] AS [AnalyticsId], " +
                               $"a.CustomerName AS [AnalyticsPartner], " +
                               $"a.LocationName AS [AnalyticsLocation], " +
                               $"a.[TransactionDate] AS [AnalyticsTransactionDate], " +
                               $"a.[OrderNo] AS [AnalyticsOrderNo], " +
                               $"a.[SubTotal] AS [AnalyticsAmount], " +
                               $"p.[Id] AS [ProofListId], " +
                               $"p.[TransactionDate] AS [ProofListTransactionDate], " +
                               $"p.[OrderNo] AS [ProofListOrderNo], " +
                               $"p.[Amount] AS [ProofListAmount],  " +
                               $"a.[IsUpload] AS [IsUpload] " +
                           $"FROM  " +
                               $"FilteredData a  " +
                           $"FULL OUTER JOIN  " +
                               $"(  " +
                                   $"SELECT  " +
                                       $"p.[Id], " +
                                       $"c.CustomerName, " +
                                       $"l.LocationName,  " +
                                       $"p.[TransactionDate],  " +
                                       $"p.[OrderNo], " +
                                       $"p.[Amount],  " +
                                       $"p.[DeleteFlag]  " +
                                  $" FROM " +
                                  $"     [dbo].[tbl_prooflist] p  " +
                                  $"     INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = p.StoreId " +
                                  $"     INNER JOIN [dbo].[tbl_customer] c ON c.CustomerCode = p.CustomerId  " +
                                  $" WHERE " +
                                  $"     (CAST(p.TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND p.StoreId = {analyticsParamsDto.storeId[0]} AND p.CustomerId LIKE '%{memCodeLast6Digits[0]}%' AND p.Amount IS NOT NULL AND p.Amount <> 0 AND p.StatusId != 4  AND p.DeleteFlag = 0)  " +
                               $") p " +
                           $"ON a.[OrderNo] = p.[OrderNo]" +
                           $"ORDER BY COALESCE(p.Id, a.Id) DESC; ")
                   .ToListAsync();

                    var groupedByOrderNo = result.GroupBy(m => m.AnalyticsOrderNo);
                    foreach (var group in groupedByOrderNo)
                    {
                        if (group.Key != null)
                        {
                            if (group.Count() > 1)
                            {
                                duplicateMatches.AddRange(group.Skip(1)); // Add duplicates to the duplicateMatches list
                            }
                        }
                        uniqueMatches.Add(group.First()); // Add the first item (unique) to the uniqueMatches list
                    }

                    formatDupes = duplicateMatches.Select(n => new MatchDto
                    {
                        AnalyticsId = n.AnalyticsId,
                        AnalyticsPartner = n.AnalyticsPartner,
                        AnalyticsLocation = n.AnalyticsLocation,
                        AnalyticsTransactionDate = n.AnalyticsTransactionDate,
                        AnalyticsOrderNo = n.AnalyticsOrderNo,
                        AnalyticsAmount = n.AnalyticsAmount,
                        ProofListId = null,
                        ProofListTransactionDate = null,
                        ProofListOrderNo = null,
                        ProofListAmount = null,
                        Variance = n.AnalyticsAmount,
                    }).ToList();

                    matchDtos = uniqueMatches.Select(m => new MatchDto
                    {
                        AnalyticsId = m.AnalyticsId,
                        AnalyticsPartner = m.AnalyticsPartner,
                        AnalyticsLocation = m.AnalyticsLocation,
                        AnalyticsTransactionDate = m.AnalyticsTransactionDate,
                        AnalyticsOrderNo = m.AnalyticsOrderNo,
                        AnalyticsAmount = m.AnalyticsAmount,
                        ProofListId = m.ProofListId,
                        ProofListTransactionDate = m.ProofListTransactionDate,
                        ProofListOrderNo = m.ProofListOrderNo,
                        ProofListAmount = m.ProofListAmount,
                        Variance = (m.AnalyticsAmount == null) ? m.ProofListAmount : (m.ProofListAmount == null) ? m.AnalyticsAmount : m.AnalyticsAmount - m.ProofListAmount.Value,
                    }).ToList();

                    matchDtos.AddRange(formatDupes);
                    orderedResult = matchDtos
                        .OrderByDescending(m => m.AnalyticsAmount == null)
                        .ThenByDescending(m => m.ProofListAmount == null)
                        .ToList();
                }

                return orderedResult;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async Task UpdateUploadStatus(AnalyticsParamsDto analyticsParamsDto)
        {
            var getMatch = await GetAnalyticsProofListVariance(analyticsParamsDto);

            if (getMatch.Where(x => x.ProofListId != null).Any())
            {
                var analyticsIdList = getMatch.Select(n => n.AnalyticsId).ToList();

                var analyticsToUpdate = await _dbContext.Analytics
                  .Where(x => analyticsIdList.Contains(x.Id))
                  .ToListAsync();

                var analyticsEntityList = analyticsToUpdate.ToList();
                analyticsEntityList.ForEach(analyticsDto =>
                {
                    analyticsDto.IsUpload = true;
                });

                var analyticsEntity = _mapper.Map<List<Analytics>>(analyticsEntityList);

                _dbContext.BulkUpdate(analyticsEntityList);
                await _dbContext.SaveChangesAsync();
            }
        }
        public async Task<int> SaveException(AnalyticsProoflistDto exceptionParam)
        {
            string clubLogs = $"{string.Join(", ", exceptionParam.refreshAnalyticsDto.storeId.Select(code => $"{code}"))}";
            string merchantLogs = $"{string.Join(", ", exceptionParam.refreshAnalyticsDto.memCode.Select(code => $"{code}"))}";
            try
            {
                var param = new AnalyticsProoflistDto
                {

                    Id = 0,
                    AnalyticsId = exceptionParam.AnalyticsId,
                    ProoflistId = 0,
                    ActionId = exceptionParam.ActionId,
                    StatusId = exceptionParam.StatusId,
                    AdjustmentId = 0,
                    SourceId = 0,
                    DeleteFlag = false,
                    AdjustmentAddDto = new AdjustmentAddDto
                    {
                        Id = 0,
                        DisputeReferenceNumber = null,
                        DisputeAmount = null,
                        DateDisputeFiled = null,
                        DescriptionOfDispute = null,
                        NewJO = null,
                        CustomerId = null,
                        AccountsPaymentDate = null,
                        AccountsPaymentTransNo = null,
                        AccountsPaymentAmount = null,
                        ReasonId = null,
                        Descriptions = null,
                        DeleteFlag = null,
                    }
                };

                var result = await CreateAnalyticsProofList(param);

                 exceptionParam.AdjustmentId = result.AdjustmentId;
                await _adjustmentService.UpdateAnalyticsProofList(exceptionParam);


                using (var newContext = _contextFactory.CreateDbContext())
                {
                    var logsDto = new LogsDto
                    {
                        UserId = exceptionParam.refreshAnalyticsDto.userId,
                        Date = DateTime.Now,
                        Action = "Save Exception",
                        Remarks = $"Success",
                        RowsCountBefore = 1,
                        RowsCountAfter = 1,
                        TotalAmount = 0,
                        Club = clubLogs,
                        CustomerId = merchantLogs
                    };
                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    newContext.Logs.Add(logsMap);
                    newContext.SaveChanges();
                }

                return result.AdjustmentId;

            }
            catch (Exception ex)
            {
                using (var newContext = _contextFactory.CreateDbContext())
                {
                    var logsDto = new LogsDto
                    {
                        UserId = exceptionParam.refreshAnalyticsDto.userId,
                        Date = DateTime.Now,
                        Action = "Save Exception",
                        Remarks = $"Error: {ex.Message}",
                        Club = clubLogs,
                        CustomerId = merchantLogs
                    };
                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    newContext.Logs.Add(logsMap);
                    newContext.SaveChanges();
                }
                return 0;

            }
        }
        public async Task RefreshAnalytics(RefreshAnalyticsDto analyticsParam)
        {
            var listResultOne = new List<Analytics>();
            string strFrom = analyticsParam.dates[0].ToString("yyMMdd");
            string strTo = analyticsParam.dates[1].ToString("yyMMdd");
            string strStamp = $"{DateTime.Now.ToString("yyMMdd")}{DateTime.Now.ToString("HHmmss")}{DateTime.Now.Millisecond.ToString()}";
            string getQuery = string.Empty;
            var deptCodeList = await GetDepartments();
            var deptCodes = string.Join(", ", deptCodeList);
            List<string> memCodeLast6Digits = analyticsParam.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
            string cstDocCondition = string.Join(" OR ", memCodeLast6Digits.Select(last6Digits => $"(CSDATE BETWEEN {strFrom} AND {strTo}) AND CSTDOC LIKE ''%{last6Digits}%''"));
            string storeList = $"CSSTOR IN ({string.Join(", ", analyticsParam.storeId.Select(code => $"{code}"))})";
            string clubLogs = $"{string.Join(", ", analyticsParam.storeId.Select(code => $"{code}"))}";
            string merchantLogs = $"{string.Join(", ", analyticsParam.memCode.Select(code => $"{code}"))}";
            int analyticsCount = 0;

            DateTime date;
            if (DateTime.TryParse(analyticsParam.dates[0].ToString(), out date))
            {
                for (int i = 0; i < analyticsParam.storeId.Count(); i++)
                {
                    for (int j = 0; j < memCodeLast6Digits.Count(); j++)
                    {
                        var analyticsToDelete = _dbContext.Analytics
                        .Where(a => a.TransactionDate == date.Date &&
                             a.CustomerId.Contains(memCodeLast6Digits[j]) &&
                             a.LocationId == analyticsParam.storeId[i]);

                        analyticsCount += analyticsToDelete.Count();

                        var portalToDelete = _dbContext.Prooflist
                         .Where(a => a.TransactionDate == date.Date &&
                                     a.CustomerId.Contains(memCodeLast6Digits[j]) &&
                                     a.StoreId == analyticsParam.storeId[i]);

                        var analyticsIdList = await analyticsToDelete.Select(n => n.Id).ToListAsync();

                        var portalIdList = await portalToDelete.Select(n => n.Id).ToListAsync();

                        _dbContext.Analytics.RemoveRange(analyticsToDelete.Where(x => x.IsTransfer == false));
                        _dbContext.SaveChanges();

                        var adjustmentAnalyticsToDelete = _dbContext.AnalyticsProoflist
                            .Where(x => analyticsIdList.Contains(x.AnalyticsId))
                            .ToList();

                        var adjustmentIdList = adjustmentAnalyticsToDelete.Select(n => n.AdjustmentId).ToList();

                        _dbContext.AnalyticsProoflist.RemoveRange(adjustmentAnalyticsToDelete);
                        _dbContext.SaveChanges();

                        var adjustmentToDelete = _dbContext.Adjustments
                           .Where(x => adjustmentIdList.Contains(x.Id))
                           .ToList();

                        _dbContext.Adjustments.RemoveRange(adjustmentToDelete);
                        _dbContext.SaveChanges();

                        var adjustmentProoflistToDelete = _dbContext.AnalyticsProoflist
                        .Where(x => portalIdList.Contains(x.ProoflistId))
                        .ToList();

                        var adjustmentPortalIdList = adjustmentProoflistToDelete.Select(n => n.AdjustmentId).ToList();

                        _dbContext.AnalyticsProoflist.RemoveRange(adjustmentProoflistToDelete);
                        _dbContext.SaveChanges();


                        var adjustmentPortalToDelete = _dbContext.Adjustments
                            .Where(x => adjustmentPortalIdList.Contains(x.Id))
                            .ToList();

                        _dbContext.Adjustments.RemoveRange(adjustmentPortalToDelete);
                        _dbContext.SaveChanges();
                    }
                }
            }

            try
            {
                await _dbContext.Database.ExecuteSqlRawAsync($"CREATE TABLE ANALYTICS_CSHTND_AR_{strStamp} (CSDATE VARCHAR(255), CSSTOR INT, CSREG INT, CSTRAN INT, CSTDOC VARCHAR(50), CSCARD VARCHAR(50), CSDTYP VARCHAR(50), CSTIL INT, CSDAMT DECIMAL(18,3))");
                await _dbContext.Database.ExecuteSqlRawAsync($"CREATE TABLE ANALYTICS_CSHTND{strStamp} (CSDATE VARCHAR(255), CSSTOR INT, CSREG INT, CSTRAN INT, CSTDOC VARCHAR(50), CSCARD VARCHAR(50), CSDTYP VARCHAR(50), CSTIL INT)");
                // Insert data from MMJDALIB.CSHTND into the newly created table ANALYTICS_CSHTND + strStamp

                int memCnt = 90;
                var rem = memCodeLast6Digits.Count() % memCnt;
                var cnt = memCodeLast6Digits.Count() / memCnt;
                int itemCnt = cnt + (rem > 0 ? 1 : 0);
                for (int x = 0; x < itemCnt; x++) {
                    string cstDocCond = "";
                    if (x == 0)
                    {
                        List<string> firstItems = memCodeLast6Digits.Take(memCnt).ToList();
                        cstDocCond = string.Join(" OR ", firstItems.Select(last6Digits => $"(CSDATE BETWEEN {strFrom} AND {strTo}) AND CSTDOC LIKE ''%{last6Digits}%'' AND {storeList}"));
                    }
                    else
                    {
                        List<string> nextItems = memCodeLast6Digits.Skip(memCnt).Take(90).ToList();
                        cstDocCond = string.Join(" OR ", nextItems.Select(last6Digits => $"(CSDATE BETWEEN {strFrom} AND {strTo}) AND CSTDOC LIKE ''%{last6Digits}%'' AND {storeList}"));

                        memCnt += memCnt;
                    }

                    await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO ANALYTICS_CSHTND{strStamp} (CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL)  " +
                                      $"SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL " +
                                      $"FROM OPENQUERY(SNR, 'SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL FROM MMJDALIB.CSHTND WHERE {cstDocCond} AND CSDTYP IN (''AR'')  " +
                                      $"GROUP BY CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL ') ");

                    await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO ANALYTICS_CSHTND_AR_{strStamp} (CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL, CSDAMT)  " +
                                    $"SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL, CSDAMT " +
                                    $"FROM OPENQUERY(SNR, 'SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL, CSDAMT FROM MMJDALIB.CSHTND WHERE {cstDocCond} AND CSDTYP IN (''AR'')  " +
                                    $"GROUP BY CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL, CSDAMT ') ");

                }


                // Create the table ANALYTICS_CSHHDR + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"CREATE TABLE ANALYTICS_CSHHDR{strStamp} (CSDATE VARCHAR(255), CSSTOR INT, CSREG INT, CSTRAN INT, CSCUST VARCHAR(255), CSTAMT DECIMAL(18,3))");
                // Insert data from MMJDALIB.CSHHDR and ANALYTICS_CSHTND into the newly created table SALES_ANALYTICS_CSHHDR + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO ANALYTICS_CSHHDR{strStamp} (CSDATE, CSSTOR, CSREG, CSTRAN, CSCUST, CSTAMT )  " +
                                  $"SELECT A.CSDATE, A.CSSTOR, A.CSREG, A.CSTRAN, A.CSCUST, A.CSTAMT  " +
                                  $"FROM OPENQUERY(SNR, 'SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSCUST, CSTAMT FROM MMJDALIB.CSHHDR WHERE (CSDATE BETWEEN {strFrom} AND {strTo}) AND {storeList} ') A  " +
                                  $"INNER JOIN ANALYTICS_CSHTND{strStamp} B  " +
                                  $"ON A.CSDATE = B.CSDATE AND A.CSSTOR = B.CSSTOR AND A.CSREG = B.CSREG AND A.CSTRAN = B.CSTRAN ");
                string test = $"SELECT A.CSDATE, A.CSSTOR, A.CSREG, A.CSTRAN, A.CSCUST, A.CSTAMT  " +
                                  $"FROM OPENQUERY(SNR, 'SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSCUST, CSTAMT FROM MMJDALIB.CSHHDR WHERE (CSDATE BETWEEN {strFrom} AND {strTo}) AND {storeList} ') A  " +
                                  $"INNER JOIN ANALYTICS_CSHTND{strStamp} B  " +
                                  $"ON A.CSDATE = B.CSDATE AND A.CSSTOR = B.CSSTOR AND A.CSREG = B.CSREG AND A.CSTRAN = B.CSTRAN ";
            }
            catch (Exception ex)
            {
                using (var newContext = _contextFactory.CreateDbContext())
                {
                    var logsDto = new LogsDto
                    {
                        UserId = analyticsParam.userId,
                        Date = DateTime.Now,
                        Action = "Refresh Analytics",
                        Remarks = $"Error: {ex.Message}",
                        Club = clubLogs,
                        CustomerId = merchantLogs
                    };
                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    newContext.Logs.Add(logsMap);
                    newContext.SaveChanges();
                }

                await DropTables(strStamp);
                throw;
            }

            try
            {
                // Create the table ANALYTICS_CONDTX + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"CREATE TABLE ANALYTICS_CONDTX{strStamp} (CSDATE VARCHAR(255), CSSTOR INT, CSREG INT, CSTRAN INT, CSSKU INT, CSQTY DECIMAL(18,3),  CSEXPR DECIMAL(18,3), CSEXCS DECIMAL(18,4), CSDSTS INT)");
                // Insert data from MMJDALIB.CONDTX into the newly created table ANALYTICS_CONDTX + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO ANALYTICS_CONDTX{strStamp} (CSDATE, CSSTOR, CSREG, CSTRAN, CSSKU, CSQTY, CSEXPR, CSEXCS, CSDSTS )  " +
                                      $"SELECT A.CSDATE, A.CSSTOR, A.CSREG, A.CSTRAN, A.CSSKU, A.CSQTY, A.CSEXPR, A.CSEXCS, A.CSDSTS  " +
                                      $"FROM OPENQUERY(SNR, 'SELECT DISTINCT CSDATE, CSSTOR, CSREG, CSTRAN, CSSKU, CSQTY, CSEXPR, CSEXCS, CSDSTS FROM MMJDALIB.CONDTX WHERE (CSDATE BETWEEN {strFrom} AND {strTo}) AND {storeList} ') A  " +
                                      $"INNER JOIN ANALYTICS_CSHTND{strStamp} B  " +
                                      $"ON A.CSDATE = B.CSDATE AND A.CSSTOR = B.CSSTOR AND A.CSREG = B.CSREG AND A.CSTRAN = B.CSTRAN ");
            }
            catch (Exception ex)
            {
                using (var newContext = _contextFactory.CreateDbContext())
                {
                    var logsDto = new LogsDto
                    {
                        UserId = analyticsParam.userId,
                        Date = DateTime.Now,
                        Action = "Refresh Analytics",
                        Remarks = $"Error: {ex.Message}",
                        Club = clubLogs,
                        CustomerId = merchantLogs
                    };
                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    newContext.Logs.Add(logsMap);
                    newContext.SaveChanges();
                }

                await DropTables(strStamp);
                throw;
            }

            try
            {
                // Create the table ANALYTICS_INVMST + strStamp

                await _dbContext.Database.ExecuteSqlRawAsync($"CREATE TABLE ANALYTICS_INVMST{strStamp} (IDESCR VARCHAR(255), IDEPT INT, ISDEPT INT, ICLAS INT, ISCLAS INT, INUMBR INT)");
                // Insert data from MMJDALIB.INVMST into the newly created table ANALYTICS_INVMST + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO ANALYTICS_INVMST{strStamp} (IDESCR, IDEPT, ISDEPT, ICLAS, ISCLAS, INUMBR) " +
                                          $"SELECT A.IDESCR, A.IDEPT, A.ISDEPT, A.ICLAS, A.ISCLAS, A.INUMBR " +
                                          $"FROM OPENQUERY(SNR, 'SELECT DISTINCT IDESCR, IDEPT, ISDEPT, ICLAS, ISCLAS, INUMBR FROM MMJDALIB.INVMST WHERE IDEPT IN ({deptCodes})') A " +
                                          $"INNER JOIN ANALYTICS_CONDTX{strStamp} B  " +
                                          $"ON A.INUMBR = B.CSSKU");
            }
            catch (Exception ex)
            {
                using (var newContext = _contextFactory.CreateDbContext())
                {
                    var logsDto = new LogsDto
                    {
                        UserId = analyticsParam.userId,
                        Date = DateTime.Now,
                        Action = "Refresh Analytics",
                        Remarks = $"Error: {ex.Message}",
                        Club = clubLogs,
                        CustomerId = merchantLogs
                    };
                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    newContext.Logs.Add(logsMap);
                    newContext.SaveChanges();
                }

                await DropTables(strStamp);
                throw;
            }

            try
            {
                // Create the table ANALYTICS_TBLSTR + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"CREATE TABLE ANALYTICS_TBLSTR{strStamp} (STRNUM INT, STRNAM VARCHAR(255))");
                // Insert data from MMJDALIB.TBLSTR into the newly created table ANALYTICS_TBLSTR + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO ANALYTICS_TBLSTR{strStamp} (STRNUM, STRNAM) " +
                                        $"SELECT * FROM OPENQUERY(SNR, 'SELECT STRNUM, STRNAM FROM MMJDALIB.TBLSTR') ");
            }
            catch (Exception ex)
            {
                using (var newContext = _contextFactory.CreateDbContext())
                {
                    var logsDto = new LogsDto
                    {
                        UserId = analyticsParam.userId,
                        Date = DateTime.Now,
                        Action = "Refresh Analytics",
                        Remarks = $"Error: {ex.Message}",
                        Club = clubLogs,
                        CustomerId = merchantLogs
                    };
                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    newContext.Logs.Add(logsMap);
                    newContext.SaveChanges();
                }

                await DropTables(strStamp);
                throw;
            }

            try
            {
                await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO [dbo].[tbl_analytics] (LocationId, TransactionDate, CustomerId, MembershipNo, CashierNo, RegisterNo, TransactionNo, OrderNo, Qty, Amount, SubTotal, UserId, DeleteFlag) " +
                                  $"SELECT C.CSSTOR, C.CSDATE, B.CSTDOC, A.CSCUST,B.CSTIL, C.CSREG, C.CSTRAN, B.CSCARD, SUM(C.CSQTY) AS CSQTY, SUM(C.CSEXPR) AS CSEXPR, B.CSDAMT, NULL AS UserId, 0 AS DeleteFlag   " +
                                  $"FROM ANALYTICS_CSHTND_AR_{strStamp} B " +
                                      $"INNER JOIN ANALYTICS_CSHHDR{strStamp} A ON A.CSSTOR = B.CSSTOR AND A.CSDATE = B.CSDATE AND A.CSREG = B.CSREG AND A.CSTRAN = B.CSTRAN  " +
                                      $"INNER JOIN ANALYTICS_CONDTX{strStamp} C ON A.CSSTOR = C.CSSTOR AND A.CSDATE = C.CSDATE AND A.CSREG = C.CSREG AND A.CSTRAN = C.CSTRAN  " +
                                     // $"INNER JOIN ANALYTICS_INVMST{strStamp} D ON C.CSSKU = D.INUMBR  " +
                                      $"INNER JOIN ANALYTICS_TBLSTR{strStamp} E ON E.STRNUM = C.CSSTOR  " +
                                  $"GROUP BY C.CSSTOR,  C.CSDATE,  B.CSTDOC,  A.CSCUST,  C.CSREG,  C.CSTRAN,  B.CSCARD,  B.CSTIL,  B.CSDAMT   " +
                                  $"ORDER BY C.CSSTOR, C.CSDATE, C.CSREG ");


                foreach (var store in analyticsParam.storeId)
                {
                    foreach (var code in analyticsParam.memCode)
                    {
                        string formattedMemCode = code.Substring(Math.Max(0, code.Length - 6));
                        if (analyticsParam.dates != null && analyticsParam.dates.Any() && analyticsParam.dates[0] != null)
                        {
                            var transactionDate = analyticsParam.dates[0].Date;

                            string sqlUpdate = @"
                                UPDATE tbl_analytics
                                SET CustomerId = @code
                                WHERE CustomerId LIKE CONCAT('%', @formattedMemCode, '%')
                                AND TransactionDate = @transactionDate
                                AND LocationId = @store";

                            await _dbContext.Database.ExecuteSqlRawAsync(sqlUpdate,
                                new SqlParameter("@code", code),
                                new SqlParameter("@formattedMemCode", formattedMemCode),
                                new SqlParameter("@transactionDate", transactionDate),
                                new SqlParameter("@store", store));
                        }
                    }
                }

                await DropTables(strStamp);

                await SubmitAnalyticsUpdate(analyticsParam);
                var analyticsParams = new AnalyticsParamsDto
                {
                    dates = analyticsParam.dates.Select(date => date.ToString()).ToList(),
                    memCode = analyticsParam.memCode,
                    userId = analyticsParam.userId,
                    storeId = analyticsParam.storeId
                };

                var toUpdate = await GetAnalyticsProofListVariance(analyticsParams);
                if (toUpdate.Where(x => x.ProofListId != null).Any())
                {
                    var analyticsIdList = toUpdate.Select(n => n.AnalyticsId).ToList();

                    var analyticsToUpdate = await _dbContext.Analytics
                      .Where(x => analyticsIdList.Contains(x.Id))
                      .ToListAsync();

                    var analyticsEntityList = analyticsToUpdate.ToList();
                    analyticsEntityList.ForEach(analyticsDto =>
                    {
                        analyticsDto.IsUpload = true;
                    });

                    var analyticsEntity = _mapper.Map<List<Analytics>>(analyticsEntityList);

                    _dbContext.BulkUpdate(analyticsEntityList);
                    await _dbContext.SaveChangesAsync();
                }

                var MatchDto = await GetMatchAnalyticsAndProofList(analyticsParam);

                var isUpload = MatchDto
                            .Where(x => x.IsUpload == true)
                            .Any();

                if (isUpload)
                {
                    foreach (var item in MatchDto)
                    {
                        var param = new AnalyticsProoflistDto
                        {

                            Id = 0,
                            AnalyticsId = item.AnalyticsId,
                            ProoflistId = item.ProofListId,
                            ActionId = null,
                            StatusId = 5,
                            AdjustmentId = 0,
                            SourceId = (item.AnalyticsId != null && item.ProofListId != null ? 1 : item.AnalyticsId != null ? 1 : item.ProofListId != null ? 2 : 0),
                            DeleteFlag = false,
                            AdjustmentAddDto = new AdjustmentAddDto
                            {
                                Id = 0,
                                DisputeReferenceNumber = null,
                                DisputeAmount = null,
                                DateDisputeFiled = null,
                                DescriptionOfDispute = null,
                                NewJO = null,
                                CustomerId = null,
                                AccountsPaymentDate = null,
                                AccountsPaymentTransNo = null,
                                AccountsPaymentAmount = null,
                                ReasonId = null,
                                Descriptions = null,
                                DeleteFlag = null,
                            }
                        };

                        var result = await CreateAnalyticsProofList(param);
                    }
                }

                var analytics = await _dbContext.Analytics
                 .Where(a => a.TransactionDate == date &&
                             a.CustomerId.Contains(memCodeLast6Digits[0]) &&
                             a.LocationId == analyticsParam.storeId[0])
                 .ToListAsync();

                var analyticsNewRows = analytics.Count();
                var totalAmount = analytics.Sum(x => x.SubTotal);

                using (var newContext = _contextFactory.CreateDbContext())
                {
                    var logsDto = new LogsDto
                    {
                        UserId = analyticsParam.userId,
                        Date = DateTime.Now,
                        Action = "Refresh Analytics",
                        Remarks = $"Success",
                        RowsCountBefore = analyticsCount,
                        RowsCountAfter = analyticsNewRows,
                        TotalAmount = totalAmount,
                        Club = clubLogs,
                        CustomerId = merchantLogs
                    };
                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    newContext.Logs.Add(logsMap);
                    newContext.SaveChanges();
                }
            }
            catch (Exception ex)
            {
                using (var newContext = _contextFactory.CreateDbContext())
                {
                    var logsDto = new LogsDto
                    {
                        UserId = analyticsParam.userId,
                        Date = DateTime.Now,
                        Action = "Refresh Analytics",
                        Remarks = $"Error: {ex.Message}",
                        Club = clubLogs,
                        CustomerId = merchantLogs
                    };
                    var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    newContext.Logs.Add(logsMap);
                    newContext.SaveChanges();
                }
               
                await DropTables(strStamp);
                throw;
            }
        }

        public async Task<AnalyticsProoflist> CreateAnalyticsProofList(AnalyticsProoflistDto adjustmentTypeDto)
        {
            var analyticsProoflist = new AnalyticsProoflist();
            var adjustmentId = await CreateAdjustment(adjustmentTypeDto.AdjustmentAddDto);

            if (adjustmentId != 0)
            {
                adjustmentTypeDto.AdjustmentId = adjustmentId;

                analyticsProoflist = _mapper.Map<AnalyticsProoflistDto, AnalyticsProoflist>(adjustmentTypeDto);
                _dbContext.AnalyticsProoflist.Add(analyticsProoflist);
                await _dbContext.SaveChangesAsync();

                return analyticsProoflist;
            }

            return analyticsProoflist;
        }

        public async Task<int> CreateAdjustment(AdjustmentAddDto? adjustmentAddDto)
        {
            try
            {
                var id = 0;
                if (adjustmentAddDto != null)
                {
                    var adjustments = _mapper.Map<AdjustmentAddDto, Adjustments>(adjustmentAddDto);
                    _dbContext.Adjustments.Add(adjustments);
                    await _dbContext.SaveChangesAsync();

                    id = adjustments.Id;

                    return id;
                }
                return id;
            }
            catch (Exception ex)
            {
                var message = ex.Message;
                throw;
            }
        }

        public async Task<List<MatchDto>> GetMatchAnalyticsAndProofList(RefreshAnalyticsDto analyticsParamsDto)
        {
            try
            {
                DateTime date;
                var matchDto = new List<MatchDto>();
                var matchDtos = new List<MatchDto>();
                var uniqueMatches = new List<CSI.Domain.Entities.Match>();
                var duplicateMatches = new List<CSI.Domain.Entities.Match>();
                var formatDupes = new List<MatchDto>();
                var orderedResult = new List<MatchDto>();
                List<string> memCodeLast6Digits = analyticsParamsDto.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
                if (DateTime.TryParse(analyticsParamsDto.dates[0].ToString(), out date))
                {
                    var result = await _dbContext.Match
                     .FromSqlRaw($"WITH RankedData AS ( " +
                                $"SELECT  " +
                                $"     MAX(a.Id) AS Id, " +
                                $"     MAX(a.LocationName) AS LocationName, " +
                                $"     MAX(a.CustomerName) AS CustomerName, " +
                                $"     MAX(a.TransactionDate) AS TransactionDate, " +
                                $"     a.OrderNo, " +
                                $"     MAX(CAST(a.IsUpload AS INT)) AS IsUpload, " +
                                $"     MAX(a.SubTotal) AS SubTotal  " +
                                $" FROM ( " +
                                $"     SELECT   " +
                                $"        n.[Id], " +
                                $"        n.LocationId, " +
                                $"        n.CustomerId, " +
                                $"        c.CustomerName, " +
                                $"        l.LocationName, " +
                                $"        n.[TransactionDate], " +
                                $"        n.[OrderNo], " +
                                $"        n.[SubTotal], " +
                                $"        n.[IsUpload],   " +
                                $"        n.[DeleteFlag],   " +
                                $"        ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                                $"     FROM tbl_analytics n " +
                                $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                                $"        INNER JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                                $"     WHERE  " +
                                $"        (CAST(TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND LocationId = {analyticsParamsDto.storeId[0]} AND CustomerId LIKE '%{memCodeLast6Digits[0]}%' AND n.DeleteFlag = 0) " +
                                $" ) a " +
                                $" GROUP BY  " +
                                $"     a.OrderNo,    " +
                                $"     ABS(a.SubTotal),  " +
                                $"     a.row_num " +
                                $" HAVING " +
                                $"     COUNT(a.OrderNo) = 1 " +
                                $"), " +
                                $"FilteredData AS ( " +
                                $"SELECT " +
                                $"    Id, " +
                                $"    CustomerName, " +
                                $"    LocationName, " +
                                $"    [TransactionDate], " +
                                $"    [OrderNo], " +
                                $"    [SubTotal], " +
                                $"    [IsUpload] " +
                                $"FROM RankedData " +
                                $") " +
                                $"SELECT " +
                                $"a.[Id] AS [AnalyticsId], " +
                                $"a.CustomerName AS [AnalyticsPartner], " +
                                $"a.LocationName AS [AnalyticsLocation], " +
                                $"a.[TransactionDate] AS [AnalyticsTransactionDate], " +
                                $"a.[OrderNo] AS [AnalyticsOrderNo], " +
                                $"a.[SubTotal] AS [AnalyticsAmount], " +
                                $"p.[Id] AS [ProofListId], " +
                                $"p.[TransactionDate] AS [ProofListTransactionDate], " +
                                $"p.[OrderNo] AS [ProofListOrderNo], " +
                                $"p.[Amount] AS [ProofListAmount],  " +
                                $"a.[IsUpload] AS [IsUpload] " +
                            $"FROM  " +
                                $"FilteredData a  " +
                            $"FULL OUTER JOIN  " +
                                $"(  " +
                                    $"SELECT  " +
                                        $"p.[Id], " +
                                        $"c.CustomerName, " +
                                        $"l.LocationName,  " +
                                        $"p.[TransactionDate],  " +
                                        $"p.[OrderNo], " +
                                        $"p.[Amount],  " +
                                        $"p.[DeleteFlag]   " +
                                   $" FROM " +
                                   $"     [dbo].[tbl_prooflist] p  " +
                                   $"     INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = p.StoreId " +
                                   $"     INNER JOIN [dbo].[tbl_customer] c ON c.CustomerCode = p.CustomerId  " +
                                   $" WHERE " +
                                   $"     (CAST(p.TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND p.StoreId = {analyticsParamsDto.storeId[0]} AND p.CustomerId LIKE '%{memCodeLast6Digits[0]}%' AND p.Amount IS NOT NULL AND p.Amount <> 0 AND p.StatusId != 4 AND p.DeleteFlag = 0)  " +
                                $") p " +
                            $"ON a.[OrderNo] = p.[OrderNo] " +
                            $"ORDER BY COALESCE(p.Id, a.Id) DESC; ")
                    .ToListAsync();

                    var groupedByOrderNo = result.GroupBy(m => m.AnalyticsOrderNo);
                    foreach (var group in groupedByOrderNo)
                    {
                        if (group.Count() > 1)
                        {
                            duplicateMatches.AddRange(group.Skip(1));
                        }
                        uniqueMatches.Add(group.First());
                    }

                    formatDupes = duplicateMatches.Select(n => new MatchDto
                    {
                        AnalyticsId = n.AnalyticsId,
                        AnalyticsPartner = n.AnalyticsPartner,
                        AnalyticsLocation = n.AnalyticsLocation,
                        AnalyticsTransactionDate = n.AnalyticsTransactionDate,
                        AnalyticsOrderNo = n.AnalyticsOrderNo,
                        AnalyticsAmount = n.AnalyticsAmount,
                        ProofListId = null,
                        ProofListTransactionDate = null,
                        ProofListOrderNo = null,
                        ProofListAmount = null,
                        Variance = n.AnalyticsAmount,
                        IsUpload = Convert.ToBoolean(n.IsUpload),
                    }).ToList();

                    matchDtos = uniqueMatches.Select(m => new MatchDto
                    {
                        AnalyticsId = m.AnalyticsId,
                        AnalyticsPartner = m.AnalyticsPartner,
                        AnalyticsLocation = m.AnalyticsLocation,
                        AnalyticsTransactionDate = m.AnalyticsTransactionDate,
                        AnalyticsOrderNo = m.AnalyticsOrderNo,
                        AnalyticsAmount = m.AnalyticsAmount,
                        ProofListId = m.ProofListId,
                        ProofListTransactionDate = m.ProofListTransactionDate,
                        ProofListOrderNo = m.ProofListOrderNo,
                        ProofListAmount = m.ProofListAmount,
                        Variance = (m.AnalyticsAmount == null) ? m.ProofListAmount : (m.ProofListAmount == null) ? m.AnalyticsAmount : m.AnalyticsAmount - m.ProofListAmount.Value,
                        IsUpload = Convert.ToBoolean(m.IsUpload),
                    }).ToList();

                    matchDtos.AddRange(formatDupes);
                    orderedResult = matchDtos
                        .OrderByDescending(m => m.AnalyticsAmount == null)
                        .ThenByDescending(m => m.ProofListAmount == null)
                        .ToList();

                    matchDto = orderedResult
                        .Where(x => x.ProofListId == null || x.AnalyticsId == null || x.Variance <= -1 || x.Variance >= 1)
                        .ToList();
                }


                return matchDto;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        private async Task DropTables(string strStamp)
        {
            try
            {

                using (var newContext = _contextFactory.CreateDbContext())
                {
                    if (newContext.Database.GetDbConnection().State == ConnectionState.Closed)
                    {
                        await newContext.Database.GetDbConnection().OpenAsync();
                    }

                    var tableNames = new[]
                    {
                        $"ANALYTICS_CSHTND{strStamp}",
                        $"ANALYTICS_CSHHDR{strStamp}",
                        $"ANALYTICS_CONDTX{strStamp}",
                        $"ANALYTICS_INVMST{strStamp}",
                        $"ANALYTICS_TBLSTR{strStamp}"
                    };

                    foreach (var tableName in tableNames)
                    {
                        await newContext.Database.ExecuteSqlRawAsync($"IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].{tableName}') AND type in (N'U')) DROP TABLE [dbo].{tableName}");
                    }

                    await newContext.Database.GetDbConnection().CloseAsync();
                }
            }
            catch (Exception ex)
            {
                await _dbContext.Database.GetDbConnection().CloseAsync();
                throw;
            }
        }
        public async Task<bool> SubmitAnalyticsWOProoflist(AnalyticsParamsDto analyticsParamsDto) 
        {
            string clubLogs = $"{string.Join(", ", analyticsParamsDto.storeId.Select(code => $"{code}"))}";
            string merchantLogs = $"{string.Join(", ", analyticsParamsDto.memCode.Select(code => $"{code}"))}";
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var isPending = true;
                var result = await ReturnAnalytics(analyticsParamsDto);

                if (result == null || result.Count() == 0)
                {
                    return false;
                }

                foreach (var analytics in result)
                {
                    analytics.StatusId = 3;
                }

                var analyticsEntityList = result.Select(analyticsDto =>
                {
                    var analyticsEntity = _mapper.Map<Analytics>(analyticsDto);
                    analyticsEntity.StatusId = 3;
                    analyticsEntity.LocationId = analyticsParamsDto.storeId[0];
                    return analyticsEntity;
                }).ToList();

                _dbContext.BulkUpdate(analyticsEntityList);
                await _dbContext.SaveChangesAsync();

                logsDto = new LogsDto
                {
                    UserId = analyticsParamsDto.userId,
                    Date = DateTime.Now,
                    Action = "Submit Analytics",
                    Remarks = $"Success",
                    RowsCountAfter = analyticsEntityList.Count(),
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return isPending;
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = analyticsParamsDto.userId,
                    Date = DateTime.Now,
                    Action = "Submit Analytics",
                    Remarks = $"Error: {ex.Message}",
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }
        public async Task<bool> SubmitAnalytics(AnalyticsParamsDto analyticsParamsDto)
        {
            string clubLogs = $"{string.Join(", ", analyticsParamsDto.storeId.Select(code => $"{code}"))}";
            string merchantLogs = $"{string.Join(", ", analyticsParamsDto.memCode.Select(code => $"{code}"))}";
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var isPending = true;
                var result = await ReturnAnalytics(analyticsParamsDto);

                var CheckIfUpload = result.Where(x => x.IsUpload == true).Any();

                if (!CheckIfUpload)
                {
                    logsDto = new LogsDto
                    {
                        UserId = analyticsParamsDto.userId,
                        Date = DateTime.Now,
                        Action = "Submit Analytics",
                        Remarks = $"Error: Can't submit analytics.",
                        Club = clubLogs,
                        CustomerId = merchantLogs
                    };
                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();

                    return false;
                }

                foreach (var analytics in result)
                {
                    analytics.StatusId = 3;
                }

                var analyticsEntityList = result.Select(analyticsDto =>
                {
                    var analyticsEntity = _mapper.Map<Analytics>(analyticsDto);
                    analyticsEntity.StatusId = 3;
                    analyticsEntity.LocationId = analyticsParamsDto.storeId[0];
                    return analyticsEntity;
                }).ToList();

                _dbContext.BulkUpdate(analyticsEntityList);
                await _dbContext.SaveChangesAsync();

                logsDto = new LogsDto
                {
                    UserId = analyticsParamsDto.userId,
                    Date = DateTime.Now,
                    Action = "Submit Analytics",
                    Remarks = $"Success",
                    RowsCountAfter = analyticsEntityList.Count(),
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return isPending;
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = analyticsParamsDto.userId,
                    Date = DateTime.Now,
                    Action = "Submit Analytics",
                    Remarks = $"Error: {ex.Message}",
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<bool> SubmitAnalyticsUpdate(RefreshAnalyticsDto analyticsParam)
        {
            var isPending = true;
            var result = await ReturnAnalyticsSubmit(analyticsParam);

            foreach (var analytics in result)
            {
                analytics.StatusId = 5;
            }

            var analyticsEntityList = result.Select(analyticsDto =>
            {
                var analyticsEntity = _mapper.Map<Analytics>(analyticsDto);
                analyticsEntity.StatusId = 5;
                analyticsEntity.LocationId = analyticsParam.storeId[0];
                return analyticsEntity;
            }).ToList();

            _dbContext.BulkUpdate(analyticsEntityList);
            await _dbContext.SaveChangesAsync();

            return isPending;
        }


        public async Task<(List<InvoiceDto>, bool)> GenerateInvoiceAnalytics(AnalyticsParamsDto analyticsParamsDto)
        {
            var invoiceAnalytics = new List<InvoiceDto>();
            var isPending = false;
            DateTime currentDate = DateTime.Now;
            Random random = new Random();
            var result = await ReturnAnalytics(analyticsParamsDto);
            var merchRef = new Dictionary<string, string>();

            if (result.Count >= 1)
            {
                var total = result.Sum(x => x.SubTotal);
                var locationList = await GetLocations();

                var club = analyticsParamsDto.storeId[0];
                var trxCount = result.Count();
                var dateFormat = result.FirstOrDefault().TransactionDate?.ToString("MMddyy");

                isPending = result
                    .Where(x => x.StatusId == 5)
                    .Any();

                if (isPending)
                {
                    return (invoiceAnalytics, isPending);
                }
                else
                {
                    var lastInvoice = await _dbContext.GenerateInvoice.OrderByDescending(i => i.Id).FirstOrDefaultAsync();
                    long startingInvoiceNumber = 000000000001;

                    if (lastInvoice != null)
                    {
                        startingInvoiceNumber = Convert.ToInt64(lastInvoice.InvoiceNo) + 1;
                    }

                    long newInvoiceNumber = startingInvoiceNumber;

                    while (await _dbContext.GenerateInvoice.AnyAsync(i => i.InvoiceNo == newInvoiceNumber.ToString("000000000000")))
                    {
                        newInvoiceNumber++;
                    }

                    var formattedInvoiceNumber = newInvoiceNumber.ToString("000000000000");

                    var getShortName = locationList
                        .Where(x => x.LocationName.Contains(result.FirstOrDefault().LocationName))
                        .Select(n => new
                        {
                            n.ShortName,
                        })
                        .FirstOrDefault();

                    var GetCustomerNo = result
                             .GroupJoin(
                                 _dbContext.CustomerCodes,
                                 x => x.CustomerId,
                                 y => y.CustomerCode,
                                 (x, y) => new { x, y }
                             )
                             .SelectMany(
                                 group => group.y,
                                 (group, y) => y.CustomerNo
                             )
                             .FirstOrDefault();

                    var formatCustomerNo = GetCustomerNo.Replace("P", "").Trim();

                    var getReference = await _dbContext.Reference
                        .Where(x => x.CustomerNo == formatCustomerNo)
                        .Select(n => new
                        {
                            n.MerchReference,
                        })
                        .FirstOrDefaultAsync();

                    var invoice = new InvoiceDto
                    {
                        HDR_TRX_NUMBER = formattedInvoiceNumber,
                        HDR_TRX_DATE = result.FirstOrDefault().TransactionDate,
                        HDR_PAYMENT_TYPE = "HS",
                        HDR_BRANCH_CODE = getShortName.ShortName ?? "",
                        HDR_CUSTOMER_NUMBER = GetCustomerNo,
                        HDR_CUSTOMER_SITE = getShortName.ShortName ?? "",
                        HDR_PAYMENT_TERM = "0",
                        HDR_BUSINESS_LINE = "1",
                        HDR_BATCH_SOURCE_NAME = "POS",
                        HDR_GL_DATE = result.FirstOrDefault().TransactionDate,
                        HDR_SOURCE_REFERENCE = "HS",
                        DTL_LINE_DESC = getReference.MerchReference + club + dateFormat + "-" + trxCount,
                        DTL_QUANTITY = 1,
                        DTL_AMOUNT = total,
                        DTL_VAT_CODE = "",
                        DTL_CURRENCY = "PHP",
                        INVOICE_APPLIED = "0",
                        FILENAME = "SN" + DateTime.Now.ToString("MMddyy_hhmmss") + ".A01"
                    };

                    invoiceAnalytics.Add(invoice);

                    var formattedResult = result.FirstOrDefault();

                    var customerName = string.Empty;
                    if (formattedResult != null)
                    {
                        customerName = _dbContext.CustomerCodes
                            .Where(cc => cc.CustomerCode == formattedResult.CustomerId)
                            .Select(cc => cc.CustomerName)
                            .FirstOrDefault();
                    }

                    var generateInvoice = new GenerateInvoiceDto
                    {
                        Club = club,
                        CustomerCode = formattedResult.CustomerId,
                        CustomerNo = GetCustomerNo,
                        CustomerName = customerName,
                        InvoiceNo = formattedInvoiceNumber,
                        InvoiceDate = formattedResult.TransactionDate,
                        TransactionDate = formattedResult.TransactionDate,
                        Location = getShortName.ShortName,
                        ReferenceNo = getReference.MerchReference + club + dateFormat,
                        InvoiceAmount = total,
                        FileName = invoiceAnalytics.FirstOrDefault().FILENAME,
                    };

                    var genInvoice = _mapper.Map<GenerateInvoiceDto, GenerateInvoice>(generateInvoice);
                    _dbContext.GenerateInvoice.Add(genInvoice);
                    await _dbContext.SaveChangesAsync();

                    return (invoiceAnalytics, isPending);
                }
            }
            return (invoiceAnalytics, true);
        }

        public async Task<List<GenerateInvoice>> GetGeneratedInvoice(AnalyticsParamsDto analyticsParamsDto)
        {
            var generatedInvoice = new List<GenerateInvoice>();
            DateTime dateFrom;
            DateTime dateTo;
            List<string> memCodeLast6Digits = analyticsParamsDto.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
            if (DateTime.TryParse(analyticsParamsDto.dates[0].ToString(), out dateFrom) &&
                DateTime.TryParse(analyticsParamsDto.dates[1].ToString(), out dateTo))
            {
                if (DateTime.TryParse(analyticsParamsDto.dates[0].ToString(), out dateFrom) &&
                    DateTime.TryParse(analyticsParamsDto.dates[1].ToString(), out dateTo))
                {
                    var genInvoice = await _dbContext.GenerateInvoice
                        .Where(x => x.TransactionDate >= dateFrom.Date && x.TransactionDate <= dateTo.Date && analyticsParamsDto.storeId.Contains(x.Club) && x.CustomerCode.Contains(memCodeLast6Digits[0]))
                        .ToListAsync();

                    generatedInvoice.AddRange(genInvoice);
                }
            }

            return generatedInvoice;
        }

        public async Task<(bool, bool)> IsSubmittedGenerated(AnalyticsParamsDto analyticsParamsDto)
        {
            var isSubmitted = false;
            var isGenerated = false;
            var result = await ReturnAnalytics(analyticsParamsDto);

            isSubmitted = result
               .Where(x => x.StatusId == 3)
               .Any();

            isGenerated = result
              .Where(x => x.IsGenerate == true)
              .Any();

            return (isSubmitted, isGenerated);
        }

        public async Task<List<int>> GetClubs()
        {
            var clubs = new List<int>();
            var result = await _dbContext.Locations
                .Where(x => x.LocationName.Contains("KAREILA"))
                .Select(n => new
                {
                    n.LocationCode
                })
                .ToListAsync();

            clubs.AddRange(result.Select(x => x.LocationCode));

            return clubs;
        }

        public async Task<(List<WeeklyReportDto>, List<RecapSummaryDto>)> GenerateWeeklyReport(AnalyticsParamsDto analyticsParamsDto)
        {
            string clubLogs = $"{string.Join(", ", analyticsParamsDto.storeId.Select(code => $"{code}"))}";
            string merchantLogs = $"{string.Join(", ", analyticsParamsDto.memCode.Select(code => $"{code}"))}";
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var weeklyReportList = new List<WeeklyReportDto>();
                var recapList = new List<RecapSummaryDto>();
                DateTime dateFrom;
                DateTime dateTo;
                List<string> memCodeLast6Digits = analyticsParamsDto.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
                if (DateTime.TryParse(analyticsParamsDto.dates[0].ToString(), out dateFrom) &&
                     DateTime.TryParse(analyticsParamsDto.dates[1].ToString(), out dateTo))
                {

                    var result = await _dbContext.AnalyticsView
                           .FromSqlRaw($" SELECT  " +
                            $"     MAX(a.Id) AS Id, " +
                            $"     MAX(a.CustomerId) AS CustomerId, " +
                            $"     MAX(a.CustomerName) AS CustomerName, " +
                            $"     MAX(a.LocationId) AS LocationId, " +
                            $"     MAX(a.LocationName) AS LocationName, " +
                            $"     MAX(a.TransactionDate) AS TransactionDate, " +
                            $"     MAX(a.MembershipNo) AS MembershipNo, " +
                            $"     MAX(a.CashierNo) AS CashierNo, " +
                            $"     MAX(a.RegisterNo) AS RegisterNo, " +
                            $"     MAX(a.TransactionNo) AS TransactionNo, " +
                            $"     a.OrderNo, " +
                            $"     MAX(a.Qty) AS Qty, " +
                            $"     MAX(a.Amount) AS Amount, " +
                            $"     MAX(CAST(a.StatusId AS INT)) AS StatusId,  " +
                            $"     MAX(CAST(a.DeleteFlag AS INT)) AS DeleteFlag, " +
                            $"     MAX(CAST(a.IsUpload AS INT)) AS IsUpload, " +
                            $"     MAX(CAST(a.IsGenerate AS INT)) AS IsGenerate, " +
                            $"     MAX(CAST(a.IsTransfer AS INT)) AS IsTransfer, " +
                            $"     MAX(a.SubTotal) AS SubTotal  " +
                            $" FROM ( " +
                            $"     SELECT   " +
                            $"         n.Id, " +
                            $"         n.CustomerId,  " +
                            $"         c.CustomerName,  " +
                            $"         n.LocationId,  " +
                            $"         l.LocationName,  " +
                            $"         n.TransactionDate,   " +
                            $"         n.MembershipNo,   " +
                            $"         n.CashierNo,  " +
                            $"         n.RegisterNo,  " +
                            $"         n.TransactionNo,  " +
                            $"         n.OrderNo,  " +
                            $"         n.Qty,  " +
                            $"         n.Amount,  " +
                            $"         n.SubTotal, " +
                            $"         n.StatusId, " +
                            $"         n.DeleteFlag,   " +
                            $"         n.IsUpload,   " +
                            $"         n.IsGenerate,   " +
                            $"         n.IsTransfer,   " +
                            $"         ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                            $"     FROM tbl_analytics n " +
                            $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                            $"        INNER JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                            $"     WHERE  " +
                            $"        (CAST(TransactionDate AS DATE) BETWEEN '{dateFrom.Date.ToString("yyyy-MM-dd")}' AND '{dateTo.Date.ToString("yyyy-MM-dd")}' AND LocationId = {analyticsParamsDto.storeId[0]} AND " +
                            $" ({string.Join(" OR ", analyticsParamsDto.memCode.Select(code => $"CustomerId LIKE '%{code.Substring(Math.Max(0, code.Length - 6))}%'"))}) " +
                            $" AND n.DeleteFlag = 0 AND n.StatusId = 3) " +
                            $" ) a " +
                            $" GROUP BY  " +
                            $"     a.OrderNo,    " +
                            $"     ABS(a.SubTotal),  " +
                            $"     a.row_num " +
                            $" HAVING " +
                            $"     COUNT(a.OrderNo) = 1 " +
                            $" ORDER BY MAX(a.TransactionDate) ASC")
                           .ToListAsync();

                    weeklyReportList = result.Select(n => new WeeklyReportDto
                    {
                        LocationName = n.LocationName,
                        TransactionDate = n.TransactionDate,
                        MembershipNo = n.MembershipNo,
                        RegisterNo = n.RegisterNo,
                        TransactionNo = n.TransactionNo,
                        OrderNo = n.OrderNo,
                        Qty = n.Qty,
                        Amount = n.Amount,
                        SubTotal = n.SubTotal,
                        Member = null,
                        NonMember = null,
                        OriginalAmout = n.SubTotal,
                        AccountsPayment = "",
                        APTRX = "",
                        TotalBilled = null
                    }).ToList();

                    if (result.Any())
                    {
                        var GetCustomerNo = result
                                .GroupJoin(
                                    _dbContext.CustomerCodes,
                                    x => x.CustomerId,
                                    y => y.CustomerCode,
                                    (x, y) => new { x, y }
                                )
                                .SelectMany(
                                    group => group.y,
                                    (group, y) => y.CustomerNo
                                )
                                .FirstOrDefault();

                        var formatCustomerNo = GetCustomerNo.Replace("P", "").Trim();

                        var getReference = await _dbContext.Reference
                           .Where(x => x.CustomerNo == formatCustomerNo)
                           .Select(n => new
                           {
                               n.MerchReference,
                           })
                           .FirstOrDefaultAsync();

                        var summary = result
                        .GroupBy(r => r.TransactionDate?.Date) // Use ?.Date to handle nullable DateTime?
                        .Select(group => new RecapSummaryDto
                        {
                            DAYOFWEEK = group.Key?.ToString("ddd") ?? "N/A", // Handle null case
                            DATE = group.Key.HasValue ? group.Key.Value.ToString("M/d/yyyy") : "N/A", // Handle null case directly
                            SAAMOUNT = group.Sum(r => r.SubTotal),
                            NOOFTRX = group.Count(),
                            PERIINVOICEENTRY = group.Sum(r => r.SubTotal),
                            VARIANCE = 0, // Calculate the variance as needed
                            REMARKS = $"{getReference.MerchReference}{analyticsParamsDto.storeId[0]}{(group.Key?.ToString("MMddyy") ?? "N/A")}-{group.Count()}" // Use ?.ToString("MMdd") to handle nullable DateTime?
                        })
                        .ToList();

                        recapList.AddRange(summary);
                    }
                }
                return (weeklyReportList, recapList);
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = analyticsParamsDto.userId,
                    Date = DateTime.Now,
                    Action = analyticsParamsDto.action,
                    Remarks = $"Error: {ex.Message}",
                    Club = clubLogs,
                    CustomerId = merchantLogs,
                    Filename = analyticsParamsDto.fileName,
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<(List<AnalyticsDto>, int)> GetAnalyticsToDelete(AnalyticsToDeleteDto analyticsToDelete)
        {
            var date = new DateTime();
            IQueryable<AnalyticsDto> analytics = Enumerable.Empty<AnalyticsDto>().AsQueryable();
            var totalPages = 0;
            var formattedData = new List<AnalyticsDto>();
            var memCodeLast6Digits = analyticsToDelete.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();

            if (DateTime.TryParse(analyticsToDelete.date, out date))
            {
                string cstDocCondition = string.Join(" OR ", memCodeLast6Digits.Select(last6Digits => $"(CAST(TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND LocationId = {analyticsToDelete.storeId} AND CustomerId LIKE '%{last6Digits}%' AND OrderNo LIKE '%{analyticsToDelete.jo}%')"));
                var result = await _dbContext.AnalyticsView
                  .FromSqlRaw($" SELECT  " +
                              $"     MAX(a.Id) AS Id, " +
                              $"     MAX(a.CustomerName) AS CustomerName, " +
                              $"     MAX(a.CustomerId) AS CustomerId, " +
                              $"     MAX(a.LocationId) AS LocationId, " +
                              $"     MAX(a.LocationName) AS LocationName, " +
                              $"     MAX(a.TransactionDate) AS TransactionDate, " +
                              $"     MAX(a.MembershipNo) AS MembershipNo, " +
                              $"     MAX(a.CashierNo) AS CashierNo, " +
                              $"     MAX(a.RegisterNo) AS RegisterNo, " +
                              $"     MAX(a.TransactionNo) AS TransactionNo, " +
                              $"     a.OrderNo, " +
                              $"     MAX(a.Qty) AS Qty, " +
                              $"     MAX(a.Amount) AS Amount, " +
                              $"     MAX(CAST(a.StatusId AS INT)) AS StatusId,  " +
                              $"     MAX(CAST(a.DeleteFlag AS INT)) AS DeleteFlag, " +
                              $"     MAX(CAST(a.IsUpload AS INT)) AS IsUpload, " +
                              $"     MAX(CAST(a.IsGenerate AS INT)) AS IsGenerate, " +
                              $"     MAX(CAST(a.IsTransfer AS INT)) AS IsTransfer, " +
                              $"     MAX(a.SubTotal) AS SubTotal  " +
                              $" FROM ( " +
                              $"     SELECT   " +
                              $"         n.Id, " +
                              $"         c.CustomerName,  " +
                              $"         n.CustomerId,  " +
                              $"         n.LocationId,  " +
                              $"         l.LocationName,  " +
                              $"         n.TransactionDate,   " +
                              $"         n.MembershipNo,   " +
                              $"         n.CashierNo,  " +
                              $"         n.RegisterNo,  " +
                              $"         n.TransactionNo,  " +
                              $"         n.OrderNo,  " +
                              $"         n.Qty,  " +
                              $"         n.Amount,  " +
                              $"         n.SubTotal, " +
                              $"         n.StatusId, " +
                              $"         n.DeleteFlag,   " +
                              $"         n.IsUpload,   " +
                              $"         n.IsGenerate,   " +
                              $"         n.IsTransfer,   " +
                              $"         ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                              $"     FROM tbl_analytics n " +
                              $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                              $"        INNER JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                              $"     WHERE  " +
                              $"       {cstDocCondition}" +
                              $" ) a " +
                              $" GROUP BY  " +
                              $"     a.OrderNo,    " +
                              $"     ABS(a.SubTotal),  " +
                              $"     a.row_num " +
                              $" HAVING " +
                              $"     COUNT(a.OrderNo) = 1 "
                              )
                  .ToListAsync();

                analytics = result.Select(n => new AnalyticsDto
                {
                    Id = n.Id,
                    CustomerName = n.CustomerName,
                    LocationName = n.LocationName,
                    TransactionDate = n.TransactionDate,
                    MembershipNo = n.MembershipNo,
                    CashierNo = n.CashierNo,
                    RegisterNo = n.RegisterNo,
                    TransactionNo = n.TransactionNo,
                    OrderNo = n.OrderNo,
                    Qty = n.Qty,
                    Amount = n.Amount,
                    SubTotal = n.SubTotal,
                    DeleteFlag = Convert.ToBoolean(n.DeleteFlag),
                }).AsQueryable();

                var totalItemCount = analytics.Count();
                totalPages = (int)Math.Ceiling((double)totalItemCount / analyticsToDelete.PageSize);

                formattedData = analytics
                    .Skip((analyticsToDelete.PageNumber - 1) * analyticsToDelete.PageSize)
                    .Take(analyticsToDelete.PageSize)
                    .OrderByDescending(x => x.Id)
                    .ToList();
            }
            return (formattedData, totalPages);
        }

        public async Task<(List<AnalyticsDto>, int)> GetAnalyticsToUndoSubmit(AnalyticsUndoSubmitDto analyticsUndoSubmit)
        {
            var date = new DateTime();
            IQueryable<AnalyticsDto> analytics = Enumerable.Empty<AnalyticsDto>().AsQueryable();
            var totalPages = 0;
            var formattedData = new List<AnalyticsDto>();
            var memCodeLast6Digits = analyticsUndoSubmit.memCode.Substring(Math.Max(0, analyticsUndoSubmit.memCode.Length - 6));

            if (DateTime.TryParse(analyticsUndoSubmit.date, out date))
            {
                string cstDocCondition = $"(CAST(TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND LocationId = {analyticsUndoSubmit.storeId} AND CustomerId LIKE '%{memCodeLast6Digits}%' AND StatusId = 3)";
                var result = await _dbContext.AnalyticsView
                  .FromSqlRaw($" SELECT  " +
                              $"     MAX(a.Id) AS Id, " +
                              $"     MAX(a.CustomerName) AS CustomerName, " +
                              $"     MAX(a.CustomerId) AS CustomerId, " +
                              $"     MAX(a.LocationId) AS LocationId, " +
                              $"     MAX(a.LocationName) AS LocationName, " +
                              $"     MAX(a.TransactionDate) AS TransactionDate, " +
                              $"     MAX(a.MembershipNo) AS MembershipNo, " +
                              $"     MAX(a.CashierNo) AS CashierNo, " +
                              $"     MAX(a.RegisterNo) AS RegisterNo, " +
                              $"     MAX(a.TransactionNo) AS TransactionNo, " +
                              $"     a.OrderNo, " +
                              $"     MAX(a.Qty) AS Qty, " +
                              $"     MAX(a.Amount) AS Amount, " +
                              $"     MAX(CAST(a.StatusId AS INT)) AS StatusId,  " +
                              $"     MAX(CAST(a.DeleteFlag AS INT)) AS DeleteFlag, " +
                              $"     MAX(CAST(a.IsUpload AS INT)) AS IsUpload, " +
                              $"     MAX(CAST(a.IsGenerate AS INT)) AS IsGenerate, " +
                              $"     MAX(CAST(a.IsTransfer AS INT)) AS IsTransfer, " +
                              $"     MAX(a.SubTotal) AS SubTotal  " +
                              $" FROM ( " +
                              $"     SELECT   " +
                              $"         n.Id, " +
                              $"         c.CustomerName,  " +
                              $"         n.CustomerId,  " +
                              $"         n.LocationId,  " +
                              $"         l.LocationName,  " +
                              $"         n.TransactionDate,   " +
                              $"         n.MembershipNo,   " +
                              $"         n.CashierNo,  " +
                              $"         n.RegisterNo,  " +
                              $"         n.TransactionNo,  " +
                              $"         n.OrderNo,  " +
                              $"         n.Qty,  " +
                              $"         n.Amount,  " +
                              $"         n.SubTotal, " +
                              $"         n.StatusId, " +
                              $"         n.DeleteFlag,   " +
                              $"         n.IsUpload,   " +
                              $"         n.IsGenerate,   " +
                              $"         n.IsTransfer,   " +
                              $"         ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                              $"     FROM tbl_analytics n " +
                              $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                              $"        INNER JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                              $"     WHERE  " +
                              $"        {cstDocCondition}" +
                              $" ) a " +
                              $" GROUP BY  " +
                              $"     a.OrderNo,    " +
                              $"     ABS(a.SubTotal),  " +
                              $"     a.row_num " +
                              $" HAVING " +
                              $"     COUNT(a.OrderNo) = 1 "
                              )
                  .ToListAsync();

                analytics = result.Select(n => new AnalyticsDto
                {
                    Id = n.Id,
                    CustomerName = n.CustomerName,
                    LocationName = n.LocationName,
                    TransactionDate = n.TransactionDate,
                    MembershipNo = n.MembershipNo,
                    CashierNo = n.CashierNo,
                    RegisterNo = n.RegisterNo,
                    TransactionNo = n.TransactionNo,
                    OrderNo = n.OrderNo,
                    Qty = n.Qty,
                    Amount = n.Amount,
                    SubTotal = n.SubTotal,
                    DeleteFlag = Convert.ToBoolean(n.DeleteFlag),
                }).AsQueryable();

                var totalItemCount = analytics.Count();
                totalPages = (int)Math.Ceiling((double)totalItemCount / analyticsUndoSubmit.PageSize);

                formattedData = analytics
                    .Skip((analyticsUndoSubmit.PageNumber - 1) * analyticsUndoSubmit.PageSize)
                    .Take(analyticsUndoSubmit.PageSize)
                    .OrderByDescending(x => x.Id)
                    .ToList();
            }
            return (formattedData, totalPages);
        }

        public async Task<bool> UndoSubmitAnalytics(AnalyticsParamsDto analyticsParamsDto)
        {
            var isPending = true;
            var result = await ReturnAnalytics(analyticsParamsDto);

            var CheckIfUpload = result.Where(x => x.IsUpload == true).Any();

            if (!CheckIfUpload)
            {
                return false;
            }

            foreach (var analytics in result)
            {
                analytics.StatusId = 5;
            }

            var analyticsEntityList = result.Select(analyticsDto =>
            {
                var analyticsEntity = _mapper.Map<Analytics>(analyticsDto);
                analyticsEntity.StatusId = 5;
                analyticsEntity.LocationId = analyticsParamsDto.storeId[0];
                return analyticsEntity;
            }).ToList();

            _dbContext.BulkUpdate(analyticsEntityList);
            await _dbContext.SaveChangesAsync();

            return isPending;
        }

        public async Task<bool> DeleteAnalytics(UpdateAnalyticsDto updateAnalyticsDto)
        {
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var result = false;

                var GetAnalytics = await _dbContext.Analytics
                    .Where(x => x.Id == updateAnalyticsDto.Id)
                    .FirstOrDefaultAsync();

                if (GetAnalytics != null)
                {
                    GetAnalytics.DeleteFlag = true;
                    await _dbContext.SaveChangesAsync();
                    result = true;

                    logsDto = new LogsDto
                    {
                        UserId = updateAnalyticsDto.UserId,
                        Date = DateTime.Now,
                        Action = "Manual Delete Analytics",
                        Remarks = $"Successfully Deleted",
                        AnalyticsId = updateAnalyticsDto.Id
                    };
                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();
                }

                return result;
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = updateAnalyticsDto.UserId,
                    Date = DateTime.Now,
                    Action = "Manual Delete Analytics",
                    Remarks = $"Error: {ex.Message}",
                    AnalyticsId = updateAnalyticsDto.Id
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<bool> RevertAnalytics(UpdateAnalyticsDto updateAnalyticsDto)
        {
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var result = false;

                var GetAnalytics = await _dbContext.Analytics
                    .Where(x => x.Id == updateAnalyticsDto.Id)
                    .FirstOrDefaultAsync();

                if (GetAnalytics != null)
                {
                    GetAnalytics.DeleteFlag = false;
                    await _dbContext.SaveChangesAsync();
                    result = true;

                    logsDto = new LogsDto
                    {
                        UserId = updateAnalyticsDto.UserId,
                        Date = DateTime.Now,
                        Action = "Manual Revert Analytics",
                        Remarks = $"Successfully Reverted",
                        AnalyticsId = updateAnalyticsDto.Id
                    };
                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();
                }

                return result;
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = updateAnalyticsDto.UserId,
                    Date = DateTime.Now,
                    Action = "Manual Revert Analytics",
                    Remarks = $"Error: {ex.Message}",
                    AnalyticsId = updateAnalyticsDto.Id
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<bool> UpdateAnalytics(UpdateAnalyticsDto updateAnalyticsDto)
        {
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var result = false;

                var GetAnalytics = await _dbContext.Analytics
                    .Where(x => x.Id == updateAnalyticsDto.Id)
                    .FirstOrDefaultAsync();

                if (GetAnalytics != null)
                {
                    var GetOldCustomerId = GetAnalytics.CustomerId;
                    GetAnalytics.CustomerId = updateAnalyticsDto.CustomerId;
                    GetAnalytics.IsTransfer = true;
                    await _dbContext.SaveChangesAsync();
                    result = true;

                    logsDto = new LogsDto
                    {
                        UserId = updateAnalyticsDto.UserId,
                        Date = DateTime.Now,
                        Action = "Manual Transfer Merchant",
                        Remarks = $"Id: {updateAnalyticsDto.Id} : " +
                                  $"Customer Id: {GetOldCustomerId} -> {updateAnalyticsDto.CustomerId}, ",
                        AnalyticsId = updateAnalyticsDto.Id
                    };
                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();
                }

                return result;
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = updateAnalyticsDto.UserId,
                    Date = DateTime.Now,
                    Action = "Manual Transfer Merchant",
                    Remarks = $"Error: {ex.Message}",
                    AnalyticsId = updateAnalyticsDto.Id
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public bool CheckFolderPath(string path)
        {
            try
            {
                var result = false;
                if (Directory.Exists(path))
                {
                    result = true;
                }

                return result;
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<(string, string, string)> GenerateA0File(GenerateA0FileDto generateA0FileDto)
        {
            string clubLogs = $"{string.Join(", ", generateA0FileDto.analyticsParamsDto.storeId.Select(code => $"{code}"))}";
            string merchantLogs = $"{string.Join(", ", generateA0FileDto.analyticsParamsDto.memCode.Select(code => $"{code}"))}";
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                //var result = false;
                var fileName = "SN" + DateTime.Now.ToString("MMddyy_hhmmss") + ".A01";
                var formattedList = new List<string>();
                var invoiceNo = "";
                var invoiceAnalytics = new List<InvoiceDto>();
                var isPending = false;
                DateTime currentDate = DateTime.Now;
                Random random = new Random();
                var getGeneratedInvoice = await AccountingGenerateInvoice(generateA0FileDto);
                if (getGeneratedInvoice.Count() >= 1)
                {
                    var getSubmittedInvoice = getGeneratedInvoice
                       .Where(x => x.SubmitStatus == 3 && x.IsGenerated == false)
                       .Select(n => new
                       {
                           n.Date,
                           n.CustomerId,
                           n.LocationId,
                       })
                       .ToList();

                    if (getSubmittedInvoice.Count() == 0)
                    {
                        logsDto = new LogsDto
                        {
                            UserId = generateA0FileDto.analyticsParamsDto.userId,
                            Date = DateTime.Now,
                            Action = "Generate A01 Invoice",
                            Remarks = $"Error: Error generating invoice. Please check and try again.",
                            Club = clubLogs,
                            CustomerId = merchantLogs
                        };
                        logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                        _dbContext.Logs.Add(logsMap);
                        await _dbContext.SaveChangesAsync();
                        return ("Error generating invoice. Please check and try again.", fileName, "");
                    }

                    foreach (var item in getSubmittedInvoice)
                    {
                        var param = new AnalyticsParamsDto
                        {
                            dates = new List<string> { item.Date.ToString() },
                            memCode = new List<string> { item.CustomerId },
                            storeId = new List<int> { item.LocationId ?? 0 },
                        };

                        var result = await ReturnAnalytics(param);
                        var merchRef = new Dictionary<string, string>();

                        if (result.Count >= 1)
                        {
                            var total = result.Sum(x => x.SubTotal);
                            var locationList = await GetLocations();

                            var club = param.storeId[0];
                            var trxCount = result.Count();
                            var dateFormat = result.FirstOrDefault().TransactionDate?.ToString("MMddyy");

                            isPending = result
                                .Where(x => x.StatusId == 5)
                                .Any();

                            var lastInvoice = await _dbContext.GenerateInvoice.OrderByDescending(i => i.Id).FirstOrDefaultAsync();
                            long startingInvoiceNumber = 000000000001;

                            if (lastInvoice != null)
                            {
                                startingInvoiceNumber = Convert.ToInt64(lastInvoice.InvoiceNo) + 1;
                            }

                            long newInvoiceNumber = startingInvoiceNumber;

                            while (await _dbContext.GenerateInvoice.AnyAsync(i => i.InvoiceNo == newInvoiceNumber.ToString("000000000000")))
                            {
                                newInvoiceNumber++;
                            }

                            var formattedInvoiceNumber = newInvoiceNumber.ToString("000000000000");

                            var getShortName = locationList
                                .Where(x => x.LocationName.Contains(result.FirstOrDefault().LocationName))
                                .Select(n => new
                                {
                                    n.ShortName,
                                })
                                .FirstOrDefault();

                            var GetCustomerNo = result
                                    .GroupJoin(
                                        _dbContext.CustomerCodes,
                                        x => x.CustomerId,
                                        y => y.CustomerCode,
                                        (x, y) => new { x, y }
                                    )
                                    .SelectMany(
                                        group => group.y,
                                        (group, y) => y.CustomerNo
                                    )
                                    .FirstOrDefault();

                            var formatCustomerNo = GetCustomerNo.Replace("P", "").Trim();

                            var getReference = await _dbContext.Reference
                                .Where(x => x.CustomerNo == formatCustomerNo)
                                .Select(n => new
                                {
                                    n.MerchReference,
                                })
                                .FirstOrDefaultAsync();

                            var invoice = new InvoiceDto
                            {
                                HDR_TRX_NUMBER = formattedInvoiceNumber,
                                HDR_TRX_DATE = result.FirstOrDefault().TransactionDate,
                                HDR_PAYMENT_TYPE = "HS",
                                HDR_BRANCH_CODE = getShortName.ShortName ?? "",
                                HDR_CUSTOMER_NUMBER = GetCustomerNo,
                                HDR_CUSTOMER_SITE = getShortName.ShortName ?? "",
                                HDR_PAYMENT_TERM = "0",
                                HDR_BUSINESS_LINE = "1",
                                HDR_BATCH_SOURCE_NAME = "POS",
                                HDR_GL_DATE = result.FirstOrDefault().TransactionDate,
                                HDR_SOURCE_REFERENCE = "HS",
                                DTL_LINE_DESC = getReference.MerchReference + club + dateFormat + "-" + trxCount,
                                DTL_QUANTITY = 1,
                                DTL_AMOUNT = total,
                                DTL_VAT_CODE = "",
                                DTL_CURRENCY = "PHP",
                                INVOICE_APPLIED = "0",
                                FILENAME = fileName 
                            };

                            invoiceAnalytics.Add(invoice);

                            var formattedResult = result.FirstOrDefault();

                            var customerName = string.Empty;
                            if (formattedResult != null)
                            {
                                customerName = _dbContext.CustomerCodes
                                    .Where(cc => cc.CustomerCode == formattedResult.CustomerId)
                                    .Select(cc => cc.CustomerName)
                                    .FirstOrDefault();
                            }

                            var generateInvoice = new GenerateInvoiceDto
                            {
                                Club = club,
                                CustomerCode = formattedResult.CustomerId,
                                CustomerNo = GetCustomerNo,
                                CustomerName = customerName,
                                InvoiceNo = formattedInvoiceNumber,
                                InvoiceDate = formattedResult.TransactionDate,
                                TransactionDate = formattedResult.TransactionDate,
                                Location = getShortName.ShortName,
                                ReferenceNo = getReference.MerchReference + club + dateFormat,
                                InvoiceAmount = total,
                                FileName = invoiceAnalytics.FirstOrDefault().FILENAME,
                            };

                            var genInvoice = _mapper.Map<GenerateInvoiceDto, GenerateInvoice>(generateInvoice);
                            _dbContext.GenerateInvoice.Add(genInvoice);
                            await _dbContext.SaveChangesAsync();

                            var param1 = new GenerateA0FileDto
                            {
                                Path = "",
                                analyticsParamsDto = new AnalyticsParamsDto
                                {
                                    dates = new List<string> { item.Date.ToString() },
                                    memCode = new List<string> { item.CustomerId },
                                    storeId = new List<int> { item.LocationId ?? 0 },
                                }
                            };

                            var getAnalytics = await GetRawAnalytics(param1.analyticsParamsDto);
                            if (getAnalytics.Any())
                            {
                                getAnalytics.ForEach(analyticsDto =>
                                {
                                    analyticsDto.IsGenerate = true;
                                    analyticsDto.InvoiceNo = formattedInvoiceNumber;
                                });

                                await _dbContext.BulkUpdateAsync(getAnalytics);
                                await _dbContext.SaveChangesAsync();

                                var accountingAnalyticsList = getAnalytics.Select(analytics => new AccountingAnalytics
                                {
                                    CustomerId = analytics.CustomerId,
                                    LocationId = analytics.LocationId,
                                    TransactionDate = analytics.TransactionDate,
                                    MembershipNo = analytics.MembershipNo,
                                    CashierNo = analytics.CashierNo,
                                    RegisterNo = analytics.RegisterNo,
                                    TransactionNo = analytics.TransactionNo,
                                    OrderNo = analytics.OrderNo,
                                    Qty = analytics.Qty,
                                    Amount = analytics.Amount,
                                    SubTotal = analytics.SubTotal,
                                    InvoiceNo = analytics.InvoiceNo,
                                    DeleteFlag = false
                                }).ToList();

                                await _dbContext.AccountingAnalytics.AddRangeAsync(accountingAnalyticsList);
                                await _dbContext.SaveChangesAsync();

                                var accountingPaymentList = accountingAnalyticsList.Select(analytics => new AccountingMatchPayment 
                                {
                                    AccountingAnalyticsId = analytics.Id,
                                    AccountingProofListId = null,
                                    AccountingStatusId = 5,
                                    DeleteFlag = false,
                                    AccountingAdjustmentId = null,
                                }).ToList();

                                await _dbContext.AccountingMatchPayment.AddRangeAsync(accountingPaymentList);
                                await _dbContext.SaveChangesAsync();

                            }
                        }
                    }
                    var content = new StringBuilder();

                    foreach (var item in invoiceAnalytics)
                    {
                        var formattedTRXDate = FormatDate(item.HDR_TRX_DATE);
                        var formattedGLDate = FormatDate(item.HDR_GL_DATE);

                        var format = new
                        {
                            HDR_TRX_NUMBER = item.HDR_TRX_NUMBER,
                            HDR_TRX_DATE = formattedTRXDate,
                            HDR_PAYMENT_TYPE = item.HDR_PAYMENT_TYPE,
                            HDR_BRANCH_CODE = item.HDR_BRANCH_CODE,
                            HDR_CUSTOMER_NUMBER = item.HDR_CUSTOMER_NUMBER,
                            HDR_CUSTOMER_SITE = item.HDR_CUSTOMER_SITE,
                            HDR_PAYMENT_TERM = item.HDR_PAYMENT_TERM,
                            HDR_BUSINESS_LINE = item.HDR_BUSINESS_LINE,
                            HDR_BATCH_SOURCE_NAME = item.HDR_BATCH_SOURCE_NAME,
                            HDR_GL_DATE = formattedGLDate,
                            HDR_SOURCE_REFERENCE = item.HDR_SOURCE_REFERENCE,
                            DTL_LINE_DESC = item.DTL_LINE_DESC,
                            DTL_QUANTITY = item.DTL_QUANTITY,
                            DTL_AMOUNT = item.DTL_AMOUNT,
                            DTL_VAT_CODE = item.DTL_VAT_CODE,
                            DTL_CURRENCY = item.DTL_CURRENCY,
                            INVOICE_APPLIED = item.INVOICE_APPLIED,
                            FILENAME = item.FILENAME
                        };

                        invoiceNo = format.HDR_TRX_NUMBER;
                        content.AppendLine($"{format.HDR_TRX_NUMBER}|{format.HDR_TRX_DATE}|{format.HDR_PAYMENT_TYPE}|{format.HDR_BRANCH_CODE}|{format.HDR_CUSTOMER_NUMBER}|{format.HDR_CUSTOMER_SITE}|{format.HDR_PAYMENT_TERM}|{format.HDR_BUSINESS_LINE}|{format.HDR_BATCH_SOURCE_NAME}|{format.HDR_GL_DATE}|{format.HDR_SOURCE_REFERENCE}|{format.DTL_LINE_DESC}|{format.DTL_QUANTITY}|{format.DTL_AMOUNT}|{format.DTL_VAT_CODE}|{format.DTL_CURRENCY}|{format.INVOICE_APPLIED}|{fileName}|");
                    }

                    string filePath = Path.Combine(generateA0FileDto.Path, fileName);
                    await File.WriteAllTextAsync(filePath, content.ToString());

                    logsDto = new LogsDto
                    {
                        UserId = generateA0FileDto.analyticsParamsDto.userId,
                        Date = DateTime.Now,
                        Action = "Generate A01 Invoice",
                        Remarks = $"Invoice No: {invoiceNo} : " +
                                  $"Invoice Generated Successfully ",
                        Club = clubLogs,
                        CustomerId = merchantLogs,
                        Filename = fileName
                    };
                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();

                    return ("Invoice Generated Successfully", fileName, content.ToString());
                }
                else
                {
                    logsDto = new LogsDto
                    {
                        UserId = generateA0FileDto.analyticsParamsDto.userId,
                        Date = DateTime.Now,
                        Action = "Generate A01 Invoice",
                        Remarks = $"Error: Error generating invoice. Please check and try again.",
                        Club = clubLogs,
                        CustomerId = merchantLogs
                    };
                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();
                    return ("Error generating invoice. Please check and try again.", fileName, "");
                }
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = generateA0FileDto.analyticsParamsDto.userId,
                    Date = DateTime.Now,
                    Action = "Generate A01 Invoice",
                    Remarks = $"Error: {ex.Message}",
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public string FormatDate(DateTime? value)
        {
            string formattedDate = value?.ToString("dd-MMM-yyyy");
            return formattedDate;
        }

        public async Task<bool> IsGenerated(AnalyticsParamsDto analyticsParamsDto)
        {
            var isGenerated = false;
            var result = await ReturnAnalytics(analyticsParamsDto);

            isGenerated = result
               .Where(x => x.IsGenerate == true)
               .Any();

            return isGenerated;
        }

        public async Task ManualReload(RefreshAnalyticsDto analyticsParam)
        {
            var listResultOne = new List<Analytics>();
            string strFrom = analyticsParam.dates[0].ToString("yyMMdd");
            string strTo = analyticsParam.dates[1].ToString("yyMMdd");
            string strStamp = $"{DateTime.Now.ToString("yyMMdd")}{DateTime.Now.ToString("HHmmss")}{DateTime.Now.Millisecond.ToString()}";
            string getQuery = string.Empty;
            var deptCodeList = await GetDepartments();
            var deptCodes = string.Join(", ", deptCodeList);
            List<string> memCodeLast6Digits = analyticsParam.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
            string cstDocCondition = string.Join(" OR ", memCodeLast6Digits.Select(last6Digits => $"(CSDATE BETWEEN {strFrom} AND {strTo}) AND CSTDOC LIKE ''%{last6Digits}%''"));
            string storeList = $"CSSTOR IN ({string.Join(", ", analyticsParam.storeId.Select(code => $"{code}"))})";
            string clubLogs = $"{string.Join(", ", analyticsParam.storeId.Select(code => $"{code}"))}";
            string merchantLogs = $"{string.Join(", ", analyticsParam.memCode.Select(code => $"{code}"))}";
            int analyticsCount = 0;
            int analyticsNewRows = 0;
            decimal totalAmount = 0;

            DateTime date;
            if (DateTime.TryParse(analyticsParam.dates[0].ToString(), out date))
            {
                for (int i = 0; i < analyticsParam.storeId.Count(); i++)
                {
                    for (int j = 0; j < memCodeLast6Digits.Count(); j++)
                    {
                        var analyticsToDelete = _dbContext.Analytics
                        .Where(a => a.TransactionDate == date.Date &&
                             a.CustomerId.Contains(memCodeLast6Digits[j]) &&
                             a.LocationId == analyticsParam.storeId[i]);

                        analyticsCount += analyticsToDelete.Count();

                        var portalToDelete = _dbContext.Prooflist
                         .Where(a => a.TransactionDate == date.Date &&
                                     a.CustomerId.Contains(memCodeLast6Digits[j]) &&
                                     a.StoreId == analyticsParam.storeId[i]);

                        var analyticsIdList = await analyticsToDelete.Select(n => n.Id).ToListAsync();

                        var portalIdList = await portalToDelete.Select(n => n.Id).ToListAsync();

                        _dbContext.Analytics.RemoveRange(analyticsToDelete.Where(x => x.IsTransfer == false));
                        _dbContext.SaveChanges();

                        var adjustmentAnalyticsToDelete = _dbContext.AnalyticsProoflist
                            .Where(x => analyticsIdList.Contains(x.AnalyticsId))
                            .ToList();

                        var adjustmentIdList = adjustmentAnalyticsToDelete.Select(n => n.AdjustmentId).ToList();

                        _dbContext.AnalyticsProoflist.RemoveRange(adjustmentAnalyticsToDelete);
                        _dbContext.SaveChanges();

                        var adjustmentToDelete = _dbContext.Adjustments
                           .Where(x => adjustmentIdList.Contains(x.Id))
                           .ToList();

                        _dbContext.Adjustments.RemoveRange(adjustmentToDelete);
                        _dbContext.SaveChanges();

                        var adjustmentProoflistToDelete = _dbContext.AnalyticsProoflist
                        .Where(x => portalIdList.Contains(x.ProoflistId))
                        .ToList();

                        var adjustmentPortalIdList = adjustmentProoflistToDelete.Select(n => n.AdjustmentId).ToList();

                        _dbContext.AnalyticsProoflist.RemoveRange(adjustmentProoflistToDelete);
                        _dbContext.SaveChanges();


                        var adjustmentPortalToDelete = _dbContext.Adjustments
                            .Where(x => adjustmentPortalIdList.Contains(x.Id))
                            .ToList();

                        _dbContext.Adjustments.RemoveRange(adjustmentPortalToDelete);
                        _dbContext.SaveChanges();
                    }
                }
            }

            try
            {
                await _dbContext.Database.ExecuteSqlRawAsync($"CREATE TABLE ANALYTICS_CSHTND{strStamp} (CSDATE VARCHAR(255), CSSTOR INT, CSREG INT, CSTRAN INT, CSTDOC VARCHAR(50), CSCARD VARCHAR(50), CSDTYP VARCHAR(50), CSTIL INT)");
                // Insert data from MMJDALIB.CSHTND into the newly created table ANALYTICS_CSHTND + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO ANALYTICS_CSHTND{strStamp} (CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL)  " +
                                  $"SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL " +
                                  $"FROM OPENQUERY(SNR, 'SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL FROM MMJDALIB.CSHTND WHERE {cstDocCondition} AND CSDTYP IN (''AR'') AND {storeList}  " +
                                  $"GROUP BY CSDATE, CSSTOR, CSREG, CSTRAN, CSTDOC, CSCARD, CSDTYP, CSTIL ') ");

                // Create the table ANALYTICS_CSHHDR + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"CREATE TABLE ANALYTICS_CSHHDR{strStamp} (CSDATE VARCHAR(255), CSSTOR INT, CSREG INT, CSTRAN INT, CSCUST VARCHAR(255), CSTAMT DECIMAL(18,3))");
                // Insert data from MMJDALIB.CSHHDR and ANALYTICS_CSHTND into the newly created table SALES_ANALYTICS_CSHHDR + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO ANALYTICS_CSHHDR{strStamp} (CSDATE, CSSTOR, CSREG, CSTRAN, CSCUST, CSTAMT )  " +
                                  $"SELECT A.CSDATE, A.CSSTOR, A.CSREG, A.CSTRAN, A.CSCUST, A.CSTAMT  " +
                                  $"FROM OPENQUERY(SNR, 'SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSCUST, CSTAMT FROM MMJDALIB.CSHHDR WHERE (CSDATE BETWEEN {strFrom} AND {strTo}) AND {storeList} ') A  " +
                                  $"INNER JOIN ANALYTICS_CSHTND{strStamp} B  " +
                                  $"ON A.CSDATE = B.CSDATE AND A.CSSTOR = B.CSSTOR AND A.CSREG = B.CSREG AND A.CSTRAN = B.CSTRAN ");
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = analyticsParam.userId,
                    Date = DateTime.Now,
                    Action = "Manual Refresh Analytics",
                    Remarks = $"Error: {ex.Message}",
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                await DropTables(strStamp);
                throw;
            }

            try
            {
                // Create the table ANALYTICS_CONDTX + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"CREATE TABLE ANALYTICS_CONDTX{strStamp} (CSDATE VARCHAR(255), CSSTOR INT, CSREG INT, CSTRAN INT, CSSKU INT, CSQTY DECIMAL(18,3),  CSEXPR DECIMAL(18,3), CSEXCS DECIMAL(18,4), CSDSTS INT)");
                // Insert data from MMJDALIB.CONDTX into the newly created table ANALYTICS_CONDTX + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO ANALYTICS_CONDTX{strStamp} (CSDATE, CSSTOR, CSREG, CSTRAN, CSSKU, CSQTY, CSEXPR, CSEXCS, CSDSTS )  " +
                                      $"SELECT A.CSDATE, A.CSSTOR, A.CSREG, A.CSTRAN, A.CSSKU, A.CSQTY, A.CSEXPR, A.CSEXCS, A.CSDSTS  " +
                                      $"FROM OPENQUERY(SNR, 'SELECT CSDATE, CSSTOR, CSREG, CSTRAN, CSSKU, CSQTY, CSEXPR, CSEXCS, CSDSTS FROM MMJDALIB.CONDTX WHERE (CSDATE BETWEEN {strFrom} AND {strTo}) AND {storeList} ') A  " +
                                      $"INNER JOIN ANALYTICS_CSHTND{strStamp} B  " +
                                      $"ON A.CSDATE = B.CSDATE AND A.CSSTOR = B.CSSTOR AND A.CSREG = B.CSREG AND A.CSTRAN = B.CSTRAN WHERE A.CSSKU <> 0 AND A.CSDSTS = '0' ");
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = analyticsParam.userId,
                    Date = DateTime.Now,
                    Action = "Manual Refresh Analytics",
                    Remarks = $"Error: {ex.Message}",
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                await DropTables(strStamp);
                throw;
            }

            try
            {
                // Create the table ANALYTICS_INVMST + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"CREATE TABLE ANALYTICS_INVMST{strStamp} (IDESCR VARCHAR(255), IDEPT INT, ISDEPT INT, ICLAS INT, ISCLAS INT, INUMBR INT)");
                // Insert data from MMJDALIB.INVMST into the newly created table ANALYTICS_INVMST + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO ANALYTICS_INVMST{strStamp} (IDESCR, IDEPT, ISDEPT, ICLAS, ISCLAS, INUMBR) " +
                                          $"SELECT A.IDESCR, A.IDEPT, A.ISDEPT, A.ICLAS, A.ISCLAS, A.INUMBR " +
                                          $"FROM OPENQUERY(SNR, 'SELECT DISTINCT IDESCR, IDEPT, ISDEPT, ICLAS, ISCLAS, INUMBR FROM MMJDALIB.INVMST WHERE IDEPT IN ({deptCodes})') A " +
                                          $"INNER JOIN ANALYTICS_CONDTX{strStamp} B  " +
                                          $"ON A.INUMBR = B.CSSKU");
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = analyticsParam.userId,
                    Date = DateTime.Now,
                    Action = "Manual Refresh Analytics",
                    Remarks = $"Error: {ex.Message}",
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                await DropTables(strStamp);
                throw;
            }

            try
            {
                // Create the table ANALYTICS_TBLSTR + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"CREATE TABLE ANALYTICS_TBLSTR{strStamp} (STRNUM INT, STRNAM VARCHAR(255))");
                // Insert data from MMJDALIB.TBLSTR into the newly created table ANALYTICS_TBLSTR + strStamp
                await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO ANALYTICS_TBLSTR{strStamp} (STRNUM, STRNAM) " +
                                        $"SELECT * FROM OPENQUERY(SNR, 'SELECT STRNUM, STRNAM FROM MMJDALIB.TBLSTR') ");
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = analyticsParam.userId,
                    Date = DateTime.Now,
                    Action = "Manual Refresh Analytics",
                    Remarks = $"Error: {ex.Message}",
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                await DropTables(strStamp);
                throw;
            }

            try
            {
                //Insert the data from tbl_analytics
                await _dbContext.Database.ExecuteSqlRawAsync($"INSERT INTO [dbo].[tbl_analytics] (LocationId, TransactionDate, CustomerId, MembershipNo, CashierNo, RegisterNo, TransactionNo, OrderNo, Qty, Amount, SubTotal, UserId, DeleteFlag) " +
                                  $"SELECT C.CSSTOR, C.CSDATE, B.CSTDOC, A.CSCUST,B.CSTIL, C.CSREG, C.CSTRAN, B.CSCARD, SUM(C.CSQTY) AS CSQTY, SUM(C.CSEXPR) AS CSEXPR, A.CSTAMT, NULL AS UserId, 0 AS DeleteFlag   " +
                                  $"FROM ANALYTICS_CSHHDR{strStamp} A " +
                                      $"INNER JOIN ANALYTICS_CSHTND{strStamp} B ON A.CSSTOR = B.CSSTOR AND A.CSDATE = B.CSDATE AND A.CSREG = B.CSREG AND A.CSTRAN = B.CSTRAN  " +
                                      $"INNER JOIN ANALYTICS_CONDTX{strStamp} C ON A.CSSTOR = C.CSSTOR AND A.CSDATE = C.CSDATE AND A.CSREG = C.CSREG AND A.CSTRAN = C.CSTRAN  " +
                                      $"INNER JOIN ANALYTICS_INVMST{strStamp} D ON C.CSSKU = D.INUMBR  " +
                                      $"INNER JOIN ANALYTICS_TBLSTR{strStamp} E ON E.STRNUM = C.CSSTOR  " +
                                  $"GROUP BY C.CSSTOR,  C.CSDATE,  B.CSTDOC,  A.CSCUST,  C.CSREG,  C.CSTRAN,  B.CSCARD,  B.CSTIL,  A.CSTAMT   " +
                                  $"ORDER BY C.CSSTOR, C.CSDATE, C.CSREG ");

                foreach (var store in analyticsParam.storeId)
                {
                    foreach (var code in analyticsParam.memCode)
                    {
                        string formattedMemCode = code.Substring(Math.Max(0, code.Length - 6));
                        if (analyticsParam.dates != null && analyticsParam.dates.Any() && analyticsParam.dates[0] != null)
                        {
                            var transactionDate = analyticsParam.dates[0].Date;

                            string sqlUpdate = @"
                                UPDATE tbl_analytics
                                SET CustomerId = @code
                                WHERE CustomerId LIKE CONCAT('%', @formattedMemCode, '%')
                                AND TransactionDate = @transactionDate
                                AND LocationId = @store";

                            await _dbContext.Database.ExecuteSqlRawAsync(sqlUpdate,
                                new SqlParameter("@code", code),
                                new SqlParameter("@formattedMemCode", formattedMemCode),
                                new SqlParameter("@transactionDate", transactionDate),
                                new SqlParameter("@store", store));
                        }
                    }
                }

                await DropTables(strStamp);

                await SubmitAnalyticsUpdate(analyticsParam);
                var analyticsParams = new AnalyticsParamsDto
                {
                    dates = analyticsParam.dates.Select(date => date.ToString()).ToList(),
                    memCode = analyticsParam.memCode,
                    userId = analyticsParam.userId,
                    storeId = analyticsParam.storeId
                };

                var toUpdate = await GetAnalyticsProofListVariance(analyticsParams);
                if (toUpdate.Where(x => x.ProofListId != null).Any())
                {
                    var analyticsIdList = toUpdate.Select(n => n.AnalyticsId).ToList();

                    var analyticsToUpdate = await _dbContext.Analytics
                      .Where(x => analyticsIdList.Contains(x.Id))
                      .ToListAsync();

                    var analyticsEntityList = analyticsToUpdate.ToList();
                    analyticsEntityList.ForEach(analyticsDto =>
                    {
                        analyticsDto.IsUpload = true;
                    });

                    var analyticsEntity = _mapper.Map<List<Analytics>>(analyticsEntityList);

                    _dbContext.BulkUpdate(analyticsEntityList);
                    await _dbContext.SaveChangesAsync();
                }

                var MatchDto = await GetMatchAnalyticsAndProofList(analyticsParam);

                var isUpload = MatchDto
                            .Where(x => x.IsUpload == true)
                            .Any();

                if (isUpload)
                {
                    foreach (var item in MatchDto)
                    {
                        var param = new AnalyticsProoflistDto
                        {

                            Id = 0,
                            AnalyticsId = item.AnalyticsId,
                            ProoflistId = item.ProofListId,
                            ActionId = null,
                            StatusId = 5,
                            AdjustmentId = 0,
                            SourceId = (item.AnalyticsId != null && item.ProofListId != null ? 1 : item.AnalyticsId != null ? 1 : item.ProofListId != null ? 2 : 0),
                            DeleteFlag = false,
                            AdjustmentAddDto = new AdjustmentAddDto
                            {
                                Id = 0,
                                DisputeReferenceNumber = null,
                                DisputeAmount = null,
                                DateDisputeFiled = null,
                                DescriptionOfDispute = null,
                                NewJO = null,
                                CustomerId = null,
                                AccountsPaymentDate = null,
                                AccountsPaymentTransNo = null,
                                AccountsPaymentAmount = null,
                                ReasonId = null,
                                Descriptions = null,
                                DeleteFlag = null,
                            }
                        };

                        var result = await CreateAnalyticsProofList(param);
                    }
                }

                if (DateTime.TryParse(analyticsParam.dates[0].ToString(), out date))
                {
                    for (int i = 0; i < analyticsParam.storeId.Count(); i++)
                    {
                        for (int j = 0; j < memCodeLast6Digits.Count(); j++)
                        {
                            var analyticsToDelete = _dbContext.Analytics
                            .Where(a => a.TransactionDate == date.Date &&
                                 a.CustomerId.Contains(memCodeLast6Digits[j]) &&
                                 a.LocationId == analyticsParam.storeId[i]);

                            analyticsNewRows += analyticsToDelete.Count();
                            totalAmount += analyticsToDelete.Sum(x => x.SubTotal);
                        }
                    }
                }

                var logsDto = new LogsDto
                {
                    UserId = analyticsParam.userId,
                    Date = DateTime.Now,
                    Action = "Manual Analytics",
                    Remarks = $"Successfuly Refreshed",
                    RowsCountBefore = analyticsCount,
                    RowsCountAfter = analyticsNewRows,
                    TotalAmount = totalAmount,
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = analyticsParam.userId,
                    Date = DateTime.Now,
                    Action = "Manual Refresh Analytics",
                    Remarks = $"Error: {ex.Message}",
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                await DropTables(strStamp);
                throw;
            }
        }

        public async Task<List<AccntGenerateInvoiceDto>> AccountingGenerateInvoice(GenerateA0FileDto generateA0FileDto)
        {
            string clubLogs = $"{string.Join(", ", generateA0FileDto.analyticsParamsDto.storeId.Select(code => $"{code}"))}";
            string merchantLogs = $"{string.Join(", ", generateA0FileDto.analyticsParamsDto.memCode.Select(code => $"{code}"))}";
            try
            {
                var result = new List<AccntGenerateInvoiceDto>();
                var getClubs = await GetClubs();
                foreach (var club in getClubs)
                {
                    DateTime date;
                    if (DateTime.TryParse(generateA0FileDto.analyticsParamsDto.dates[0].ToString(), out date))
                    {
                        var GetAnalytics = _dbContext.Locations
                        .Where(location => location.LocationCode == club)
                        .GroupJoin(_dbContext.Analytics
                                .Where(analytics =>
                                    analytics.TransactionDate.Value == date.Date &&
                                    analytics.DeleteFlag == false &&
                                    generateA0FileDto.analyticsParamsDto.memCode.Contains(analytics.CustomerId)),
                            location => location.LocationCode,
                            analytics => analytics.LocationId,
                            (location, analyticsGroup) => new { location, analyticsGroup }
                        )
                        .SelectMany(
                            x => x.analyticsGroup.DefaultIfEmpty(),
                            (x, analytics) => new AccntGenerateInvoiceDto
                            {
                                Id = analytics != null ? analytics.Id : 0,
                                CustomerId = analytics != null ? analytics.CustomerId : null,
                                Date = date,
                                Location = x.location.LocationName,
                                LocationId = x.location.LocationCode,
                                SubmitStatus = analytics != null ? analytics.StatusId : 0,
                                IsGenerated = analytics.IsGenerate
                            }
                        )
                        .OrderBy(x => x.SubmitStatus)
                        .FirstOrDefault();
                        result.Add(GetAnalytics);
                    }
                }

                var logsDto = new LogsDto
                {
                    UserId = generateA0FileDto.analyticsParamsDto.userId,
                    Date = DateTime.Now,
                    Action = "Refresh Generate Invoice",
                    Remarks = $"Successfully Refreshed",
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return result;
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = generateA0FileDto.analyticsParamsDto.userId,
                    Date = DateTime.Now,
                    Action = "Refresh Generate Invoice",
                    Remarks = $"Error: {ex.Message}",
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }
        public async Task<List<DashboardAccounting>> DashboardAccounting(GenerateA0FileDto generateA0FileDto)
        {
            var result = new List<DashboardAccounting>();
            var getClubs = await GetClubs();
            var formatClubs = string.Join(", ", getClubs);
            DateTime date;
            if (DateTime.TryParse(generateA0FileDto.analyticsParamsDto.dates[0].ToString(), out date))
            {
                result = await _dbContext.DashboardAccounting
                 .FromSqlRaw($"SELECT  " +    
                     $"      l.LocationName,  " +       
                     $"       MAX(CASE WHEN a.CustomerId = '9999011955' THEN a.StatusId ELSE NULL END) AS GrabMart,   " +     
                     $"       MAX(CASE WHEN a.CustomerId = '9999011929' THEN a.StatusId ELSE NULL END) AS GrabFood,      " +   
                     $"       MAX(CASE WHEN a.CustomerId = '9999011931' THEN a.StatusId ELSE NULL END) AS [PickARooMerch],     " +   
                     $"       MAX(CASE WHEN a.CustomerId = '9999011935' THEN a.StatusId ELSE NULL END) AS [PickARooFS],  " +     
                     $"       MAX(CASE WHEN a.CustomerId = '9999011838' THEN a.StatusId ELSE NULL END) AS [FoodPanda],  " +     
                     $"       MAX(CASE WHEN a.CustomerId = '9999011855' THEN a.StatusId ELSE NULL END) AS MetroMart, " +   
                     $"       loc.LocationCode " +   
                     $"   FROM ( " +   
                     $"       SELECT LocationCode " +   
                     $"       FROM tbl_location " +   
                     $"       WHERE LocationCode IN ({formatClubs}) " +   
                     $"   ) loc " +   
                     $"   LEFT JOIN ( " +   
                     $"       SELECT DISTINCT LocationId, CustomerId, StatusId, TransactionDate, DeleteFlag " +   
                     $"       FROM tbl_analytics " +   
                     $"       WHERE CAST(TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' " +   
                     $"           AND CustomerId IN ('9999011955', '9999011929', '9999011931', '9999011935', '9999011838', '9999011855') " +   
                     $"           AND DeleteFlag = 0 " +   
                     $"   ) a ON loc.LocationCode = a.LocationId " +   
                     $"   LEFT JOIN tbl_location l ON l.LocationCode = loc.LocationCode " +   
                     $"   GROUP BY  " +   
                     $"       l.LocationName, loc.LocationCode  " +   
                     $"   ORDER BY  " +   
                     $"       loc.LocationCode ASC;") 
                 .ToListAsync();
            }

            return result;
        }

        public async Task<List<FileDescriptions>> FileDescriptions()
        {
            var fileDescriptions = new List<FileDescriptions>();

            fileDescriptions = await _dbContext.FileDescription.ToListAsync();

            return fileDescriptions;
        }

        public async Task<(List<AccountingProoflistDto>, int totalPages)> GetAccountingProoflist(PaginationDto paginationDto)
        {
            var accountingProoflistDto = new List<AccountingProoflistDto>();

            if (paginationDto.Id == null)
            {
                return (accountingProoflistDto, 0);
            }

            var prooflist = await _dbContext.AccountingProoflists
                .Where(x => x.FileDescriptionId == paginationDto.Id)
                .GroupJoin(_dbContext.Locations, x => x.StoreId, y => y.LocationCode, (x, y) => new { x, y })
                .SelectMany(
                    xy => xy.y.DefaultIfEmpty(),
                    (xy, y) => new { xy.x, Location = y }
                )
                .Join(_dbContext.CustomerCodes, c => c.x.CustomerId, p => p.CustomerCode, (c, p) => new { c, p })
                .Select(n => new AccountingProoflistDto
                {
                    Id = n.c.x.Id,
                    CustomerId = n.p.CustomerName,
                    TransactionDate = n.c.x.TransactionDate,
                    OrderNo = n.c.x.OrderNo,
                    NonMembershipFee = n.c.x.NonMembershipFee,
                    PurchasedAmount = n.c.x.PurchasedAmount,
                    Amount = n.c.x.Amount,
                    Status = n.c.x.StatusId,
                    StoreName = n.c.Location != null ? n.c.Location.LocationName : "No Location",
                    FileDescriptionId = n.c.x.FileDescriptionId,
                    DeleteFlag = n.c.x.DeleteFlag,
                })
                .ToListAsync();


            var totalItemCount = prooflist.Count();
            var totalPages = (int)Math.Ceiling((double)totalItemCount / paginationDto.PageSize);

            var sortData = prooflist
                .Skip((paginationDto.PageNumber - 1) * paginationDto.PageSize)
                .Take(paginationDto.PageSize)
                .OrderByDescending(x => x.TransactionDate)
                .ThenBy(x => x.Id)
                .ToList();

            return (sortData, totalPages);
        }

        public async Task<List<AccountingProoflistAdjustmentsDto>> GetAccountingProoflistAdjustments(PaginationDto paginationDto)
        {
            var accountingProoflistDto = new List<AccountingProoflistAdjustmentsDto>();

            if (paginationDto.Id == null)
            {
                return (accountingProoflistDto);
            }

            var prooflist = await _dbContext.AccountingProoflistAdjustments
                .Where(x => x.FileDescriptionId == paginationDto.Id)
                .GroupJoin(_dbContext.Locations, x => x.StoreId, y => y.LocationCode, (x, y) => new { x, y })
                .SelectMany(
                    xy => xy.y.DefaultIfEmpty(),
                    (xy, y) => new { xy.x, Location = y }
                )
                .Join(_dbContext.CustomerCodes, c => c.x.CustomerId, p => p.CustomerCode, (c, p) => new { c, p })
                .Select(n => new AccountingProoflistAdjustmentsDto
                {
                    Id = n.c.x.Id,
                    CustomerId = n.p.CustomerName,
                    TransactionDate = n.c.x.TransactionDate,
                    OrderNo = n.c.x.OrderNo,
                    NonMembershipFee = n.c.x.NonMembershipFee,
                    PurchasedAmount = n.c.x.PurchasedAmount,
                    Amount = n.c.x.Amount,
                    Status = n.c.x.StatusId,
                    StoreName = n.c.Location != null ? n.c.Location.LocationName : "No Location",
                    Descriptions = n.c.x.Descriptions,
                    FileDescriptionId = n.c.x.FileDescriptionId,
                    DeleteFlag = n.c.x.DeleteFlag,
                })
                .ToListAsync();


            return prooflist;
        }

        public async Task<List<AnalyticsDto>> GetAccountingAnalyitcs(AnalyticsParamsDto analyticsParamsDto)
        {
            var analyticsList = new List<AnalyticsDto>();
            DateTime date1;
            DateTime date2;
            var analytics = new List<AnalyticsDto>();
            if (DateTime.TryParse(analyticsParamsDto.dates[0].ToString(), out date1) && DateTime.TryParse(analyticsParamsDto.dates[1].ToString(), out date2))
            {
                var result = await _dbContext.AccountingAnalytics
                    .Join(_dbContext.Locations, a => a.LocationId, b => b.LocationCode, (a, b) => new { a, b })
                    .Join(_dbContext.CustomerCodes, c => c.a.CustomerId, d => d.CustomerCode, (c, d) => new { c, d })
                    .Where(x => x.c.a.TransactionDate.Value.Date >= date1.Date && x.c.a.TransactionDate.Value.Date <= date2.Date
                        && x.c.a.CustomerId == analyticsParamsDto.memCode[0]
                        && x.c.a.InvoiceNo != null
                        && x.c.a.OrderNo.Contains(analyticsParamsDto.orderNo))
                    .Select(n => new AnalyticsDto 
                    {
                        Id = n.c.a.Id,
                        CustomerId = n.d.CustomerCode,
                        LocationName = n.c.b.LocationName,
                        TransactionDate = n.c.a.TransactionDate,
                        MembershipNo = n.c.a.MembershipNo,
                        CashierNo = n.c.a.CashierNo,
                        RegisterNo = n.c.a.RegisterNo,
                        TransactionNo = n.c.a.TransactionNo,
                        OrderNo = n.c.a.OrderNo,
                        Qty = n.c.a.Qty,
                        Amount = n.c.a.Amount,
                        SubTotal = n.c.a.SubTotal,
                        DeleteFlag = Convert.ToBoolean(n.c.a.DeleteFlag),
                        InvoiceNo = n.c.a.InvoiceNo
                    })
                    .ToListAsync();

                return result;
            }
            return analytics;
        }

        public async Task<(List<AccountingMatchDto>, int totalPages)> GetAccountingProofListVariance(AnalyticsParamsDto analyticsParamsDto)
        {
            try
            {
                List<string> memCodeLast6Digits = analyticsParamsDto.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
                DateTime dateFrom;
                DateTime dateTo;
                var matchDtos = new List<AccountingMatchDto>();
                var totalItemCount = 0;
                var totalPages = 0;
                var updatedMatch = new List<AccountingMatchDto>();

                if (DateTime.TryParse(analyticsParamsDto.dates[0], out dateFrom) && DateTime.TryParse(analyticsParamsDto.dates[1], out dateTo))
                {
                    string dateFromStr = dateFrom.Date.ToString("yyyy-MM-dd");
                    string dateToStr = dateTo.Date.ToString("yyyy-MM-dd");

                    string cstDocCondition = string.Join(" OR ", memCodeLast6Digits.Select(last6Digits =>
                        $"(CAST(n.TransactionDate AS DATE) >= '{dateFromStr}' AND CAST(n.TransactionDate AS DATE) <= '{dateToStr}' AND n.CustomerId LIKE '%{last6Digits}%' AND n.OrderNo LIKE '%{analyticsParamsDto.orderNo}%' AND n.DeleteFlag = 0 AND  n.InvoiceNo IS NOT NULL AND am.DeleteFlag = 0)"));

                    string cstDocCondition1 = string.Join(" OR ", memCodeLast6Digits.Select(last6Digits =>
                        $"(CAST(p.TransactionDate AS DATE) >= '{dateFromStr}' AND CAST(p.TransactionDate AS DATE) <= '{dateToStr}' AND p.CustomerId LIKE '%{last6Digits}%' AND p.OrderNo LIKE '%{analyticsParamsDto.orderNo}%' AND p.Amount IS NOT NULL AND p.Amount <> 0 AND p.StatusId != 4 AND p.DeleteFlag = 0 AND am.DeleteFlag = 0)"));

                    var result = _dbContext.AccountingMatch
                   .FromSqlRaw($@"SELECT   
                                    am.[Id] AS [MatchId], 
	                                n.Id AS [AnalyticsId],
	                                n.[InvoiceNo] AS [AnalyticsInvoiceNo], 
                                    c.[CustomerName] AS [AnalyticsPartner],
	                                l.LocationName AS [AnalyticsLocation], 
	                                n.[TransactionDate] AS [AnalyticsTransactionDate], 
	                                n.[OrderNo] AS [AnalyticsOrderNo], 
	                                n.[SubTotal] AS [AnalyticsAmount], 
	                                p.Id AS [ProofListId],
	                                p.[Amount] AS [ProofListAmount],  
	                                p.[OrderNo] AS [ProofListOrderNo], 
	                                p.[TransactionDate] AS [ProofListTransactionDate],  
	                                l.LocationName AS [ProofListLocation], 
                                    c.[CustomerName] AS [ProofListPartner],
                                    p.AgencyFee AS [ProofListAgencyFee],
	                                ac.[StatusName] AS [Status]
                                FROM [tbl_accounting_match] am 
                                LEFT JOIN [dbo].[tbl_accounting_analytics] n ON n.Id = am.AccountingAnalyticsId
                                LEFT JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId 
                                LEFT JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId 
                                LEFT JOIN [dbo].[tbl_accounting_status] ac ON ac.Id = am.AccountingStatusId 
                                LEFT JOIN [dbo].[tbl_accounting_prooflist] p ON p.Id = am.AccountingProoflistId
                                WHERE ({cstDocCondition}) OR ({cstDocCondition1})")
                   .AsQueryable();


                    var test = $@"SELECT   
                                    am.[Id] AS [MatchId], 
	                                n.Id AS [AnalyticsId],
	                                n.[InvoiceNo] AS [AnalyticsInvoiceNo], 
                                    c.[CustomerName] AS [AnalyticsPartner],
	                                l.LocationName AS [AnalyticsLocation], 
	                                n.[TransactionDate] AS [AnalyticsTransactionDate], 
	                                n.[OrderNo] AS [AnalyticsOrderNo], 
	                                n.[SubTotal] AS [AnalyticsAmount], 
	                                p.Id AS [ProofListId],
	                                p.[Amount] AS [ProofListAmount],  
	                                p.[OrderNo] AS [ProofListOrderNo], 
	                                p.[TransactionDate] AS [ProofListTransactionDate],  
	                                l.LocationName AS [ProofListLocation], 
                                    c.[CustomerName] AS [ProofListPartner],
                                    p.AgencyFee AS [ProofListAgencyFee],
	                                ac.[StatusName] AS [Status]
                                FROM [tbl_accounting_match] am 
                                LEFT JOIN [dbo].[tbl_accounting_analytics] n ON n.Id = am.AccountingAnalyticsId
                                LEFT JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId 
                                LEFT JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId 
                                LEFT JOIN [dbo].[tbl_accounting_status] ac ON ac.Id = am.AccountingStatusId 
                                LEFT JOIN [dbo].[tbl_accounting_prooflist] p ON p.Id = am.AccountingProoflistId
                                WHERE ({cstDocCondition}) OR ({cstDocCondition1})";
                    matchDtos = result.Select(m => new AccountingMatchDto
                    {
                        MatchId = m.MatchId,
                        AnalyticsId = m.AnalyticsId,
                        AnalyticsInvoiceNo = m.AnalyticsInvoiceNo,
                        AnalyticsPartner = m.AnalyticsPartner,
                        AnalyticsLocation = m.AnalyticsLocation,
                        AnalyticsTransactionDate = m.AnalyticsTransactionDate,
                        AnalyticsOrderNo = m.AnalyticsOrderNo,
                        AnalyticsAmount = m.AnalyticsAmount,
                        ProofListId = m.ProofListId,
                        Status = m.Status.ToUpper(),
                        ProofListTransactionDate = m.ProofListTransactionDate,
                        ProofListOrderNo = m.ProofListOrderNo,
                        ProofListAmount = m.ProofListAmount,
                        ProofListPartner = m.ProofListPartner,
                        ProofListLocation = m.ProofListLocation,
                        ProofListAgencyFee = m.ProofListAgencyFee,
                        Variance = (m.AnalyticsAmount == null) ? m.ProofListAmount : (m.ProofListAmount == null) ? m.AnalyticsAmount : m.AnalyticsAmount - m.ProofListAmount.Value,
                    }).ToList();

                    totalItemCount = matchDtos.Count();
                    totalPages = (int)Math.Ceiling((double)totalItemCount / (double)analyticsParamsDto.PageSize);

                    updatedMatch = matchDtos
                        .Skip((int)((analyticsParamsDto.PageNumber - 1) * analyticsParamsDto.PageSize))
                        .Take((int)analyticsParamsDto.PageSize)
                        .ToList();

                    if (analyticsParamsDto.status.Count != 0)
                    {
                        if (analyticsParamsDto.status[0] == "All")
                        {
                            analyticsParamsDto.status = new List<string> { "Paid", "Underpaid", "Overpaid", "Not Reported", "Unpaid", "Adjustments", "Re-Transact", "Paid w/AP", "Unpaid w/AP", "Underpaid w/AP", "Overpaid w/AP" };

                            matchDtos = matchDtos
                               .Where(x => analyticsParamsDto.status.Any(status => x.Status.Trim().ToLower().Contains(status.Trim().ToLower())))
                               .OrderByDescending(m => m.AnalyticsAmount == null)
                               .ThenByDescending(m => m.ProofListAmount == null)
                               .ToList();

                            totalItemCount = matchDtos.Count();
                            totalPages = (int)Math.Ceiling((double)totalItemCount / (double)analyticsParamsDto.PageSize);

                            updatedMatch = matchDtos
                                .Skip((int)((analyticsParamsDto.PageNumber - 1) * analyticsParamsDto.PageSize))
                                .Take((int)analyticsParamsDto.PageSize)
                                .ToList();

                            return (updatedMatch, totalPages);
                        }
                        else
                        {
                            matchDtos = matchDtos
                               .Where(x => x.Status.Trim().ToLower() == analyticsParamsDto.status[0].Trim().ToLower())
                               .OrderByDescending(m => m.AnalyticsAmount == null)
                               .ThenByDescending(m => m.ProofListAmount == null)
                               .ToList();

                            totalItemCount = matchDtos.Count();
                            totalPages = (int)Math.Ceiling((double)totalItemCount / (double)analyticsParamsDto.PageSize);

                            updatedMatch = matchDtos
                                .Skip((int)((analyticsParamsDto.PageNumber - 1) * analyticsParamsDto.PageSize))
                                .Take((int)analyticsParamsDto.PageSize)
                                .ToList();

                            return (updatedMatch, totalPages);
                        }
                    }
                    return (updatedMatch, totalPages);
                }
                return (updatedMatch, 0);
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async Task<List<ExceptionReportDto>> ExportExceptions(RefreshAnalyticsDto refreshAnalyticsDto)
        {
            string clubLogs = $"{string.Join(", ", refreshAnalyticsDto.storeId.Select(code => $"{code}"))}";
            string merchantLogs = $"{string.Join(", ", refreshAnalyticsDto.memCode.Select(code => $"{code}"))}";
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                DateTime dateFrom;
                DateTime dateTo;
                var query = new List<ExceptionReportDto>();
                if (DateTime.TryParse(refreshAnalyticsDto.dates[0].ToString(), out dateFrom) && DateTime.TryParse(refreshAnalyticsDto.dates[1].ToString(), out dateTo))
                {
                    if (refreshAnalyticsDto.memCode.Count >= 1 )
                    {
                        string cstDocCondition = string.Join(" OR ", refreshAnalyticsDto.memCode.Select(last6Digits =>
                            $"(CAST(a.TransactionDate AS DATE) >= '{dateFrom.Date.ToString("yyyy-MM-dd")}' AND " +
                            $"CAST(a.TransactionDate AS DATE) <= '{dateTo.Date.ToString("yyyy-MM-dd")}' AND " +
                            $"a.CustomerId LIKE '%{last6Digits}%' AND " +
                            $"a.LocationId IN ({string.Join(",", refreshAnalyticsDto.storeId)}) AND " +
                            $"a.DeleteFlag = 0 )"));
                        string cstDocCondition1 = string.Join(" OR ", refreshAnalyticsDto.memCode.Select(last6Digits =>
                            $"(CAST(p.TransactionDate AS DATE) >= '{dateFrom.Date.ToString("yyyy-MM-dd")}' AND " +
                            $"CAST(p.TransactionDate AS DATE) <= '{dateTo.Date.ToString("yyyy-MM-dd")}' AND " +
                            $"p.CustomerId LIKE '%{last6Digits}%' AND " +
                            $"p.StoreId IN ({string.Join(",", refreshAnalyticsDto.storeId)}) AND " +
                            $"p.DeleteFlag = 0 )"));
                        var result = await _dbContext.AdjustmentExceptions
                           .FromSqlRaw($"SELECT ap.Id, c.CustomerName, a.OrderNo, a.TransactionDate, a.SubTotal, act.Action, " +
                                    $"so.SourceType, st.StatusName, ap.AdjustmentId, lo.LocationName, ap.AnalyticsId, ap.ProoflistId, " +
                                    $"adj.OldJO, a.OrderNo AS [NewJO], adj.CustomerIdOld, a.CustomerId AS [CustomerIdNew], adj.DisputeReferenceNumber, adj.DisputeAmount, adj.DateDisputeFiled, adj.DescriptionOfDispute, " +
                                    $"adj.AccountsPaymentDate, adj.AccountsPaymentTransNo, adj.AccountsPaymentAmount,  adj.ReasonId, re.ReasonDesc, " +
                                    $"adj.Descriptions " +
                                    $"FROM [dbo].[tbl_analytics_prooflist] ap " +
                                    $"	LEFT JOIN [dbo].[tbl_analytics] a ON a.Id = ap.AnalyticsId " +
                                    $"	LEFT JOIN [dbo].[tbl_prooflist] p ON p.Id = ap.ProoflistId " +
                                    $"	LEFT JOIN [dbo].[tbl_customer] c ON c.CustomerCode = a.CustomerId " +
                                    $"	LEFT JOIN [dbo].[tbl_action] act ON act.Id = ap.ActionId " +
                                    $"	LEFT JOIN [dbo].[tbl_adjustments] adj ON adj.Id = ap.AdjustmentId " +
                                    $"	LEFT JOIN [dbo].[tbl_status] st ON st.Id = ap.StatusId " +
                                    $"	LEFT JOIN [dbo].[tbl_source] so ON so.Id = ap.SourceId " +
                                    $"	LEFT JOIN [dbo].[tbl_location] lo ON lo.LocationCode = a.LocationId " +
                                    $"	LEFT JOIN [dbo].[tbl_reason] re ON re.Id = adj.ReasonId " +
                                    $"WHERE {cstDocCondition} " +
                                    $"UNION ALL " +
                                    $"SELECT ap.Id, c.CustomerName, p.OrderNo, p.TransactionDate, p.Amount, act.Action,  " +
                                    $"	so.SourceType, st.StatusName, ap.AdjustmentId, lo.LocationName, ap.AnalyticsId, ap.ProoflistId, " +
                                    $"	adj.OldJO, a.OrderNo AS [NewJO], adj.CustomerIdOld, a.CustomerId AS [CustomerIdNew], adj.DisputeReferenceNumber, adj.DisputeAmount, adj.DateDisputeFiled, adj.DescriptionOfDispute, " +
                                    $"	adj.AccountsPaymentDate, adj.AccountsPaymentTransNo, adj.AccountsPaymentAmount,  adj.ReasonId, re.ReasonDesc, " +
                                    $"	adj.Descriptions " +
                                    $"FROM [dbo].[tbl_analytics_prooflist] ap " +
                                    $"	LEFT JOIN [dbo].[tbl_analytics] a ON a.Id = ap.AnalyticsId " +
                                    $"	LEFT JOIN [dbo].[tbl_prooflist] p ON p.Id = ap.ProoflistId " +
                                    $"	LEFT JOIN [dbo].[tbl_customer] c ON c.CustomerCode = p.CustomerId " +
                                    $"	LEFT JOIN [dbo].[tbl_action] act ON act.Id = ap.ActionId " +
                                    $"	LEFT JOIN [dbo].[tbl_adjustments] adj ON adj.Id = ap.AdjustmentId " +
                                    $"	LEFT JOIN [dbo].[tbl_status] st ON st.Id = ap.StatusId " +
                                    $"	LEFT JOIN [dbo].[tbl_source] so ON so.Id = ap.SourceId " +
                                    $"	LEFT JOIN [dbo].[tbl_location] lo ON lo.LocationCode = p.StoreId " +
                                    $"	LEFT JOIN [dbo].[tbl_reason] re ON re.Id = adj.ReasonId " +
                                    $"WHERE {cstDocCondition1} " +
                                    $" ORDER BY so.SourceType, a.SubTotal ASC ")
                           .ToListAsync();

                        query = result.Select(m => new ExceptionReportDto
                        {
                            Id = m.Id,
                            CustomerId = m.CustomerName,
                            JoNumber = m.OrderNo,
                            TransactionDate = m.TransactionDate,
                            Amount = m.SubTotal,
                            AdjustmentType = m.Action,
                            Source = m.SourceType,
                            Status = m.StatusName,
                            LocationName = m.LocationName,
                            OldJo = m.OldJO,
                            NewJo = m.NewJO,
                            OldCustomerId = m.CustomerIdOld,
                            NewCustomerId = m.CustomerIdNew,
                            DisputeReferenceNumber = m.DisputeReferenceNumber,
                            DisputeAmount = m.DisputeAmount,
                            DateDisputeFiled = m.DateDisputeFiled,
                            DescriptionOfDispute = m.DescriptionOfDispute,
                            AccountsPaymentDate = m.AccountsPaymentDate,
                            AccountsPaymentTransNo = m.AccountsPaymentTransNo,
                            AccountsPaymentAmount = m.AccountsPaymentAmount,
                            ReasonDesc = m.ReasonDesc,
                            Descriptions = m.Descriptions
                        })
                        .OrderBy(x => x.TransactionDate)
                        .ThenBy(x => x.CustomerId)
                        .ThenBy(x => x.LocationName)
                        .ToList();
                    }
                }
                return query;
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = refreshAnalyticsDto.userId,
                    Date = DateTime.Now,
                    Action = "Exception Report",
                    Remarks = $"Error: {ex.Message}",
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<Analytics> CreateAnalytics(AnalyticsAddDto createAnalyticsDto)
        {
            string userId = createAnalyticsDto.UserId.ToString();
            var analytics = new Analytics();
            DateTime date;
            try
            {
                if (DateTime.TryParse(createAnalyticsDto.TransactionDate.ToString(), out date))
                {

                    createAnalyticsDto.UserId = null;
                    createAnalyticsDto.DeleteFlag = false;
                    createAnalyticsDto.IsTransfer = true;
                    createAnalyticsDto.IsGenerate = false;
                    createAnalyticsDto.TransactionDate = date.Date;

                    var isUpload = await _dbContext.Analytics
                                  .Where(x => x.IsUpload == true && x.CustomerId == createAnalyticsDto.CustomerId && x.LocationId == createAnalyticsDto.LocationId && x.TransactionDate.Value.Date == date.Date)
                                  .AnyAsync();

                    createAnalyticsDto.IsUpload = isUpload;

                     analytics = _mapper.Map<AnalyticsAddDto, Analytics>(createAnalyticsDto);
                    _dbContext.Analytics.Add(analytics);
                    await _dbContext.SaveChangesAsync();
                }

                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = createAnalyticsDto.AnalyticsParamsDto.action,
                    Remarks = $"Successfully Added",
                    Club = createAnalyticsDto.LocationId.ToString(),
                    CustomerId = createAnalyticsDto.CustomerId,
                    AnalyticsId = analytics.Id,
                };
                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return analytics;
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = createAnalyticsDto.AnalyticsParamsDto.action,
                    Remarks = $"Error: {ex.Message}",
                    Club = createAnalyticsDto.LocationId.ToString(),
                    CustomerId = createAnalyticsDto.CustomerId
                };
                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<List<Logs>> GetLogs()
        {
            try
            {
                var logList = await _dbContext.Logs.ToListAsync();
                return logList;
            }
            catch (Exception)
            {

                throw;
            }
        }

        public void InsertLogs(RefreshAnalyticsDto refreshAnalyticsDto)
        {
            string clubLogs = $"{string.Join(", ", refreshAnalyticsDto.storeId.Select(code => $"{code}"))}";
            string merchantLogs = $"{string.Join(", ", refreshAnalyticsDto.memCode.Select(code => $"{code}"))}";
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                logsDto = new LogsDto
                {
                    UserId = refreshAnalyticsDto.userId,
                    Date = DateTime.Now,
                    Action = refreshAnalyticsDto.action,
                    Remarks = refreshAnalyticsDto.remarks != string.Empty || refreshAnalyticsDto.remarks != null ? refreshAnalyticsDto.remarks : $"Successfully Generated",
                    Club = clubLogs,
                    CustomerId = merchantLogs,
                    Filename = refreshAnalyticsDto.fileName,
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                _dbContext.SaveChanges(); 
            }
            catch (Exception)
            {

                throw;
            }
        }

        public async Task<List<VarianceMMS>> GetVarianceMMS(RefreshAnalyticsDto refreshAnalyticsDto)
        {
            string clubLogs = $"{string.Join(", ", refreshAnalyticsDto.storeId.Select(code => $"{code}"))}";
            string merchantLogs = $"{string.Join(", ", refreshAnalyticsDto.memCode.Select(code => $"'{code}'"))}";
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                DateTime dateFrom;
                var query = new List<VarianceMMS>();
                if (DateTime.TryParse(refreshAnalyticsDto.dates[0].ToString(), out dateFrom))
                {
                    var formattedDate = dateFrom.ToString("yyMMdd");
                    var result = await _dbContext.VarianceMMS.
                        FromSqlRaw($"SELECT A.MMS, SUM(ABS(A.MMS - B.CSI))[Variance], B.CSI " +
                            $"FROM (SELECT CSRPAM[MMS] FROM OPENQUERY([SNR],'SELECT * FROM MMJDALIB.CSHREP WHERE CSDATE = {formattedDate} " +
                            $"AND CSSTOR = {refreshAnalyticsDto.storeId[0]} AND CSTLIN = 720 AND CSREG = 0 AND CSTIL = 0')) AS [A] " +
                            $"CROSS JOIN (SELECT SUM(SubTotal)[CSI] " +
                            $"FROM (SELECT LocationId, FORMAT(CAST(TransactionDate AS DATE),'yyMMdd')[TransactionDate], SUM(SubTotal)[SubTotal] " +
                            $"FROM tbl_analytics WHERE DeleteFlag = 0 AND CustomerId IN ({merchantLogs}) GROUP BY LocationId, TransactionDate, SubTotal)[Z] " +
                            $"WHERE LocationId = {refreshAnalyticsDto.storeId[0]} AND TransactionDate = {formattedDate}) AS [B] GROUP BY A.MMS, B.CSI")
                        .ToListAsync();

                    query = result.Select(m => new VarianceMMS
                    {
                        MMS = m.MMS,
                        CSI = m.CSI,
                        Variance = m.Variance
                    }).ToList();
                }
                return query;
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = refreshAnalyticsDto.userId,
                    Date = DateTime.Now,
                    Action = "Variance MMS",
                    Remarks = $"Error: {ex.Message}",
                    Club = clubLogs,
                    CustomerId = merchantLogs
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<bool> UpdateAccountingAdjustments(AccountingAdjustmentDto accountingAdjustmentDto)
        {
            string userId = accountingAdjustmentDto.analyticsParamsDto.userId.ToString();
            var locationId = accountingAdjustmentDto.analyticsParamsDto.storeId[0];
            var customerId = accountingAdjustmentDto.analyticsParamsDto.memCode[0];
            var accountingAdj = new AccountingAdjustments();
            DateTime date;
            var result = false;
            try
            {
                accountingAdj = _mapper.Map<AccountingAdjustmentDto, AccountingAdjustments>(accountingAdjustmentDto);
                _dbContext.AccountingAdjustments.Add(accountingAdj);
                await _dbContext.SaveChangesAsync();

                if (accountingAdjustmentDto.AccountingAdjustmentTypeId == 1)
                {
                    var getAccountingMatch = _dbContext.AccountingMatchPayment
                        .Where(x => x.Id == accountingAdjustmentDto.MatchId)
                        .FirstOrDefault();

                    if (getAccountingMatch != null)
                    {
                        getAccountingMatch.AccountingAnalyticsId = null;
                        getAccountingMatch.AccountingStatusId = 4;
                        getAccountingMatch.AccountingAdjustmentId = accountingAdj.Id;
                        await _dbContext.SaveChangesAsync();
                    }

                    var getAccountingAnalytics = _dbContext.AccountingAnalytics
                        .Where(x => x.Id == accountingAdjustmentDto.AccountingAnalyticsId)
                        .ToList();

                    if (getAccountingAnalytics != null)
                    {
                        foreach (var analytics in getAccountingAnalytics)
                        {
                            analytics.DeleteFlag = true;
                        }
                        await _dbContext.SaveChangesAsync();

                        var accountingPaymentList = getAccountingAnalytics.Select(analytics => new AccountingMatchPayment
                        {
                            AccountingAnalyticsId = analytics.Id,
                            AccountingProofListId = null,
                            AccountingStatusId = 7,
                            DeleteFlag = false,
                        }).ToList();

                        await _dbContext.AccountingMatchPayment.AddRangeAsync(accountingPaymentList);
                        await _dbContext.SaveChangesAsync();
                    }

                    result = true;
                }
                else if (accountingAdjustmentDto.AccountingAdjustmentTypeId == 2)
                {
                    var getAccountingMatch = _dbContext.AccountingMatchPayment
                       .Where(x => x.Id == accountingAdjustmentDto.MatchId)
                       .FirstOrDefault();

                    if (getAccountingMatch != null)
                    {
                        getAccountingMatch.AccountingProofListId = accountingAdjustmentDto.AccountingProofListId;
                        await _dbContext.SaveChangesAsync();

                        var getAccountingMatchStatus = _dbContext.AccountingMatchPayment
                            .Where(x => x.Id == accountingAdjustmentDto.MatchId)
                            .Join(_dbContext.AccountingAnalytics, x => x.AccountingAnalyticsId, y => y.Id, (x, y) => new { x, y })
                            .Join(_dbContext.AccountingProoflists, a => a.x.AccountingProofListId, b => b.Id, (a, b) => new { a, b })
                            .Select(n => new
                            {
                                StatusId = (n.a.y.SubTotal == null) ? 4 :
                                            (n.b.Amount == null) ? 5 :
                                            n.a.y.SubTotal == n.b.Amount ? 1 :
                                            n.a.y.SubTotal > n.b.Amount ? 2 :
                                            n.a.y.SubTotal < n.b.Amount ? 3 : 4
                            })
                            .FirstOrDefault();

                        if (getAccountingMatchStatus != null)
                        {
                            getAccountingMatch.AccountingStatusId = getAccountingMatchStatus.StatusId;
                            getAccountingMatch.AccountingAdjustmentId = accountingAdj.Id;
                            await _dbContext.SaveChangesAsync();

                            var updateDeleteFlag = _dbContext.AccountingMatchPayment
                            .Where(x => x.Id == accountingAdjustmentDto.ProofListMatchId)
                            .FirstOrDefault();

                            if (updateDeleteFlag != null)
                            {
                                updateDeleteFlag.DeleteFlag = true;
                                await _dbContext.SaveChangesAsync();
                            }

                            return true;
                        }
                    }
                }
                else
                {
                    decimal? totalAmount = 0;
                    var getAccountingMatch = _dbContext.AccountingMatchPayment
                        .Where(x => x.Id == accountingAdjustmentDto.MatchId)
                        .FirstOrDefault();

                    if (getAccountingMatch != null) 
                    {
                        if (getAccountingMatch.AccountingProofListId != null)
                        {
                            var getAccountingAnalytics = _dbContext.AccountingAnalytics
                              .Where(x => x.Id == getAccountingMatch.AccountingAnalyticsId)
                              .FirstOrDefault();

                            var getAccountingProofList = _dbContext.AccountingProoflists
                               .Where(x => x.Id == getAccountingMatch.AccountingProofListId)
                               .FirstOrDefault();

                            totalAmount = accountingAdjustmentDto.Amount + getAccountingProofList.Amount;

                            var getAccountingMatchStatus = _dbContext.AccountingMatchPayment
                            .Where(x => x.Id == accountingAdjustmentDto.MatchId)
                            .Join(_dbContext.AccountingAnalytics, x => x.AccountingAnalyticsId, y => y.Id, (x, y) => new { x, y })
                            .Select(n => new
                            {
                                StatusId = (n.y.SubTotal == null) ? 9 :
                                            n.y.SubTotal == totalAmount ? 8 :
                                            n.y.SubTotal > totalAmount ? 10 :
                                            n.y.SubTotal < totalAmount ? 11 : 8
                            })
                            .FirstOrDefault();

                            if (getAccountingMatchStatus != null)
                            {
                                getAccountingMatch.AccountingStatusId = getAccountingMatchStatus.StatusId;
                                getAccountingMatch.AccountingAdjustmentId = accountingAdj.Id;
                                await _dbContext.SaveChangesAsync();

                                return true;
                            }
                        }
                        else
                        {
                            totalAmount = accountingAdjustmentDto.Amount;

                            var getAccountingMatchStatus = _dbContext.AccountingMatchPayment
                            .Where(x => x.Id == accountingAdjustmentDto.MatchId)
                            .Join(_dbContext.AccountingAnalytics, x => x.AccountingAnalyticsId, y => y.Id, (x, y) => new { x, y })
                            .Select(n => new
                            {
                                StatusId = (n.y.SubTotal == null) ? 9 :
                                            n.y.SubTotal == totalAmount ? 8 :
                                            n.y.SubTotal > totalAmount ? 10 :
                                            n.y.SubTotal < totalAmount ? 11 : 8
                            })
                            .FirstOrDefault();

                            if (getAccountingMatchStatus != null)
                            {
                                getAccountingMatch.AccountingStatusId = getAccountingMatchStatus.StatusId;
                                getAccountingMatch.AccountingAdjustmentId = accountingAdj.Id;
                                await _dbContext.SaveChangesAsync();

                                return true;
                            }
                        }
                    }
                }

                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Update Accounting Adjustment",
                    Remarks = $"Successfully Updated",
                    Club = locationId.ToString(),
                    CustomerId = customerId,
                    AnalyticsId = accountingAdjustmentDto.AccountingAnalyticsId,
                };
                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();

                return result;
            }
            catch (Exception ex)
            {
                var logsDto = new LogsDto
                {
                    UserId = userId,
                    Date = DateTime.Now,
                    Action = "Update Accounting Adjustment",
                    Remarks = $"Error: {ex.Message}",
                    Club = locationId.ToString(),
                    CustomerId = customerId
                };
                var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<List<AccountingMatchPaymentDto>> GetAccountingPaymentProofList(AnalyticsParamsDto analyticsParamsDto)
        {
            try
            {
                List<string> memCodeLast6Digits = analyticsParamsDto.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
                DateTime dateFrom;
                DateTime dateTo;
                var matchDtos = new List<AccountingMatchPaymentDto>();

                if (DateTime.TryParse(analyticsParamsDto.dates[0], out dateFrom))
                {
                    string dateFromStr = dateFrom.Date.ToString("yyyy-MM-dd");

                    string cstDocCondition1 = string.Join(" OR ", memCodeLast6Digits.Select(last6Digits =>
                        $"(CAST(p.TransactionDate AS DATE) >= '{dateFromStr}' AND am.AccountingStatusId = 4 AND p.CustomerId LIKE '%{last6Digits}%' AND p.OrderNo = '{analyticsParamsDto.orderNo}' AND p.Amount IS NOT NULL AND p.Amount <> 0 AND p.StatusId != 4 AND p.DeleteFlag = 0)"));

                    var result = await _dbContext.AccountingProofListPayment
                   .FromSqlRaw($@"SELECT   
	                                am.[Id] AS [MatchId], 
	                                am.AccountingAnalyticsId AS [AnalyticsId],
	                                am.AccountingProofListId AS [ProofListId],
	                                ac.[StatusName] AS [Status],
	                                p.[TransactionDate] AS [TransactionDate],  
	                                p.[OrderNo] AS [OrderNo], 
	                                p.[Amount] AS [Amount],  
	                                l.LocationName AS [Location]
                                FROM [tbl_accounting_match] am 
                                LEFT JOIN [dbo].[tbl_accounting_prooflist] p ON p.Id = am.AccountingProoflistId
                                LEFT JOIN [dbo].[tbl_location] l ON l.LocationCode = p.StoreId 
                                LEFT JOIN [dbo].[tbl_customer] c ON c.CustomerCode = p.CustomerId 
                                LEFT JOIN [dbo].[tbl_accounting_status] ac ON ac.Id = am.AccountingStatusId 
                                WHERE ({cstDocCondition1})")
                   .ToListAsync();

                    matchDtos = result.Select(m => new AccountingMatchPaymentDto
                    {
                        MatchId = m.MatchId,
                        AnalyticsId = m.AnalyticsId,
                        ProofListId = m.ProofListId,
                        Status = m.Status,
                        TransactionDate = m.TransactionDate,
                        OrderNo = m.OrderNo,
                        Amount = m.Amount,
                        Location = m.Location,
                    }).ToList();
                }
                return matchDtos;
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async Task<AccountingAdjustments> GetAdjustments(int Id)
        {
            try
            {
                var adjustments = new AccountingAdjustments();

                if (Id != 0)
                {
                    var getAdjId = await _dbContext.AccountingMatchPayment
                        .Where(x => x.Id == Id)
                        .FirstOrDefaultAsync();

                    if (getAdjId != null)
                    {
                        adjustments = await _dbContext.AccountingAdjustments
                       .Where(x => x.Id == getAdjId.AccountingAdjustmentId)
                       .FirstOrDefaultAsync();
                    }
                }
                return adjustments;
            }
            catch (Exception ex)
            {
                throw;
            }
        }
    }
}
