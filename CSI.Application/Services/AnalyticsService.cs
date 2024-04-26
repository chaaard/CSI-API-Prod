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
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace CSI.Application.Services
{
    public class AnalyticsService : IAnalyticsService
    {
        private readonly AppDBContext _dbContext;
        private readonly IConfiguration _configuration;
        private readonly IMapper _mapper;

        public AnalyticsService(IConfiguration configuration, AppDBContext dBContext, IMapper mapper)
        {
            _configuration = configuration;
            _dbContext = dBContext;
            _mapper = mapper;
            _dbContext.Database.SetCommandTimeout(999);

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
                          $"     (CAST(TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND LocationId = {analyticsParamsDto.storeId[0]} AND CustomerId LIKE '%{memCodeLast6Digits[0]}%' AND n.DeleteFlag = 0) " +
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

            DateTime date;
            if (DateTime.TryParse(analyticsParam.dates[0].ToString(), out date))
            {
                var analyticsToDelete = _dbContext.Analytics
                   .Where(a => a.TransactionDate == date &&
                               a.CustomerId.Contains(memCodeLast6Digits[0]) &&
                               a.LocationId == analyticsParam.storeId[0]);

                var portalToDelete = _dbContext.Prooflist
                 .Where(a => a.TransactionDate == date &&
                             a.CustomerId.Contains(memCodeLast6Digits[0]) &&
                             a.StoreId == analyticsParam.storeId[0]);

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
                await DropTables(strStamp);
                throw;
            }

            try
            {
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
            }
            catch (Exception ex)
            {
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
                if (_dbContext.Database.GetDbConnection().State == ConnectionState.Closed)
                {
                    await _dbContext.Database.GetDbConnection().OpenAsync();
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
                    await _dbContext.Database.ExecuteSqlRawAsync($"IF OBJECT_ID('{tableName}', 'U') IS NOT NULL DROP TABLE {tableName}");
                }

                await _dbContext.Database.GetDbConnection().CloseAsync();
            }
            catch (Exception ex)
            {
                await _dbContext.Database.GetDbConnection().CloseAsync();
                throw;
            }
        }

        public async Task<bool> SubmitAnalytics(AnalyticsParamsDto analyticsParamsDto)
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

            return isPending;
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
                        $"        (CAST(TransactionDate AS DATE) BETWEEN '{dateFrom.Date.ToString("yyyy-MM-dd")}' AND '{dateTo.Date.ToString("yyyy-MM-dd")}' AND LocationId = {analyticsParamsDto.storeId[0]} AND CustomerId LIKE '%{memCodeLast6Digits[0]}%' AND n.DeleteFlag = 0 AND n.StatusId = 3) " +
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

        public async Task<bool> DeleteAnalytics(int id)
        {
            var result = false;

            var GetAnalytics = await _dbContext.Analytics
                .Where(x => x.Id == id)
                .FirstOrDefaultAsync();

            if (GetAnalytics != null)
            {
                GetAnalytics.DeleteFlag = true;
                await _dbContext.SaveChangesAsync();
                result = true;
            }

            return result;
        }

        public async Task<bool> RevertAnalytics(int id)
        {
            var result = false;

            var GetAnalytics = await _dbContext.Analytics
                .Where(x => x.Id == id)
                .FirstOrDefaultAsync();

            if (GetAnalytics != null)
            {
                GetAnalytics.DeleteFlag = false;
                await _dbContext.SaveChangesAsync();
                result = true;
            }

            return result;
        }

        public async Task<bool> UpdateAnalytics(UpdateAnalyticsDto updateAnalyticsDto)
        {
            var result = false;

            var GetAnalytics = await _dbContext.Analytics
                .Where(x => x.Id == updateAnalyticsDto.Id)
                .FirstOrDefaultAsync();



            if (GetAnalytics != null)
            {
                GetAnalytics.CustomerId = updateAnalyticsDto.CustomerId;
                GetAnalytics.IsTransfer = true;
                await _dbContext.SaveChangesAsync();
                result = true;
            }

            return result;
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
            try
            {
                //var result = false;
                var fileName = "";
                var formattedList = new List<string>();

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
                                });

                                _dbContext.BulkUpdate(getAnalytics);
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


                        fileName = format.FILENAME;
                        content.AppendLine($"{format.HDR_TRX_NUMBER}|{format.HDR_TRX_DATE}|{format.HDR_PAYMENT_TYPE}|{format.HDR_BRANCH_CODE}|{format.HDR_CUSTOMER_NUMBER}|{format.HDR_CUSTOMER_SITE}|{format.HDR_PAYMENT_TERM}|{format.HDR_BUSINESS_LINE}|{format.HDR_BATCH_SOURCE_NAME}|{format.HDR_GL_DATE}|{format.HDR_SOURCE_REFERENCE}|{format.DTL_LINE_DESC}|{format.DTL_QUANTITY}|{format.DTL_AMOUNT}|{format.DTL_VAT_CODE}|{format.DTL_CURRENCY}|{format.INVOICE_APPLIED}|{format.FILENAME}|");
                    }

                    string filePath = Path.Combine(generateA0FileDto.Path, fileName);
                    await File.WriteAllTextAsync(filePath, content.ToString());

                    return ("Invoice Generated Successfully", fileName, content.ToString());
                }
                else
                {
                    return ("Error generating invoice. Please check and try again.", fileName, "");
                }
            }
            catch (Exception)
            {

                throw;
            }
        }

        static void ExecuteBatchFile(string filePath)
        {
            Process process = new Process();
            process.StartInfo.FileName = filePath;
            process.Start();
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
            }
            catch (Exception ex)
            {
                await DropTables(strStamp);
                throw;
            }
        }

        public async Task<List<AccntGenerateInvoiceDto>> AccountingGenerateInvoice(GenerateA0FileDto generateA0FileDto)
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
                    .GroupJoin(
                        _dbContext.Analytics
                            .Where(analytics =>
                                analytics.TransactionDate.Value == date.Date &&
                                analytics.DeleteFlag == false &&
                                analytics.CustomerId.Contains(generateA0FileDto.analyticsParamsDto.memCode[0])),
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
                    .OrderBy( x => x.SubmitStatus)
                    .FirstOrDefault();
                    result.Add(GetAnalytics);
                }
            }

            return result;
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

    }
}
