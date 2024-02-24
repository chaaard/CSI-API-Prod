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
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;

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
                          $"         ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                          $"     FROM tbl_analytics n " +
                          $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                          $"        LEFT JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                          $" ) a " +
                          $" WHERE  " +
                          $"     (CAST(a.TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND a.LocationId = {analyticsParamsDto.storeId[0]} AND a.CustomerId LIKE '%{memCodeLast6Digits[0]}%' AND a.DeleteFlag = 0) " +
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
                          $"         ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                          $"     FROM tbl_analytics n " +
                          $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                          $"        LEFT JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                          $" ) a " +
                          $" WHERE  " +
                          $"     (CAST(a.TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND a.LocationId = {analyticsParamsDto.storeId[0]} AND a.CustomerId LIKE '%{memCodeLast6Digits[0]}%' AND a.DeleteFlag = 0) " +
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
                         $"         ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                         $"     FROM tbl_analytics n " +
                         $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                         $"        LEFT JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                         $" ) a " +
                         $" WHERE  " +
                         $"     (CAST(a.TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND a.LocationId = {refreshAnalyticsDto.storeId[0]} AND a.CustomerId LIKE '%{memCodeLast6Digits[0]}%') AND a.DeleteFlag = 0" +
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
                    DeleteFlag = Convert.ToBoolean(n.DeleteFlag),
                }).ToList();
            }

            return analytics;
        }

        public async Task<decimal?> GetTotalAmountPerMechant(AnalyticsParamsDto analyticsParamsDto)
        {
            DateTime date;
            decimal? result = 0;
            if (DateTime.TryParse(analyticsParamsDto.dates[0], out date))
            {
                result = await _dbContext.Analytics
                    .Where(x => x.TransactionDate == date && x.LocationId == analyticsParamsDto.storeId[0] && analyticsParamsDto.memCode[0].Contains(x.CustomerId) && x.DeleteFlag == false)
                    .SumAsync(e => e.SubTotal);
            }
            return result;
        }


        public async Task<List<MatchDto>> GetAnalyticsProofListVariance(AnalyticsParamsDto analyticsParamsDto)
        {
            try
            {
                List<string> memCodeLast6Digits = analyticsParamsDto.memCode.Select(code => code.Substring(Math.Max(0, code.Length - 6))).ToList();
                DateTime date;
                var matchDtos = new List<MatchDto>();
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
                               $" ) a " +
                               $" WHERE  " +
                               $"      (CAST(a.TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND a.LocationId = {analyticsParamsDto.storeId[0]} AND a.CustomerId LIKE '%{memCodeLast6Digits[0]}%' AND a.DeleteFlag = 0) " +
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

                    matchDtos = result.Select(m => new MatchDto
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
                }

                return matchDtos;
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
                               a.CustomerId.Contains(memCodeLast6Digits[0])  &&
                               a.LocationId == analyticsParam.storeId[0]);

                var portalToDelete = _dbContext.Prooflist
                 .Where(a => a.TransactionDate == date &&
                             a.CustomerId.Contains(memCodeLast6Digits[0]) &&
                             a.StoreId == analyticsParam.storeId[0]);

                var analyticsIdList = await analyticsToDelete.Select(n => n.Id).ToListAsync();

                var portalIdList = await portalToDelete.Select(n => n.Id).ToListAsync();

                _dbContext.Analytics.RemoveRange(analyticsToDelete);
                _dbContext.SaveChanges();

                var adjustmentAnalyticsToDelete =  _dbContext.AnalyticsProoflist
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
                                $" ) a " +
                                $" WHERE  " +
                                $"      (CAST(a.TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND a.LocationId = {analyticsParamsDto.storeId[0]} AND a.CustomerId LIKE '%{memCodeLast6Digits[0]}%'  AND a.DeleteFlag = 0) " +
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
                            $"ON a.[OrderNo] = p.[OrderNo]" +
                            $"ORDER BY COALESCE(p.Id, a.Id) DESC; ")
                    .ToListAsync();

                    var matchDtos = result.Select(m => new MatchDto
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

                    matchDto = matchDtos
                        .Where(x => x.ProofListId == null || x.AnalyticsId == null || x.Variance <= -2 || x.Variance >= 2)
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

            if (result != null)
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
                        .Select(n => new {
                            n.MerchReference,
                        })
                        .FirstOrDefaultAsync();

                    var invoice = new InvoiceDto
                    {
                        HDR_TRX_NUMBER = formattedInvoiceNumber,
                        HDR_TRX_DATE = result.FirstOrDefault().TransactionDate,
                        HDR_PAYMENT_TYPE = "HS",
                        HDR_BRANCH_CODE = getShortName.ShortName ?? "",
                        HDR_CUSTOMER_NUMBER = result.FirstOrDefault().CustomerId,
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
                        CustomerNo = formattedResult.CustomerId,
                        CustomerName = customerName,
                        InvoiceNo = formattedInvoiceNumber,
                        InvoiceDate = formattedResult.TransactionDate,
                        TransactionDate = formattedResult.TransactionDate,
                        Location = formattedResult.LocationName,
                        ReferenceNo = getReference.MerchReference + club + dateFormat,
                        InvoiceAmount = total,
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

        public async Task<bool> IsSubmitted(AnalyticsParamsDto analyticsParamsDto)
        {
            var isSubmitted = false;
            var result = await ReturnAnalytics(analyticsParamsDto);

            isSubmitted = result
               .Where(x => x.StatusId == 3)
               .Any();

            return isSubmitted;
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
                        $"         ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                        $"     FROM tbl_analytics n " +
                        $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                        $"        LEFT JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                        $" ) a " +
                        $" WHERE  " +
                        $" (CAST(a.TransactionDate AS DATE) BETWEEN '{dateFrom.Date.ToString("yyyy-MM-dd")}' AND '{dateTo.Date.ToString("yyyy-MM-dd")}' AND a.LocationId = {analyticsParamsDto.storeId[0]} AND a.CustomerId LIKE '%{memCodeLast6Digits[0]}%'  AND a.DeleteFlag = 0 ) " +
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
                        REMARKS = $"GEI{analyticsParamsDto.storeId[0]}{(group.Key?.ToString("MMddyy") ?? "N/A")}-{group.Count()}" // Use ?.ToString("MMdd") to handle nullable DateTime?
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
            var memCodeLast6Digits = analyticsToDelete.memCode.Substring(Math.Max(0, analyticsToDelete.memCode.Length - 6));
            if (DateTime.TryParse(analyticsToDelete.date, out date))
            {
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
                              $"         ROW_NUMBER() OVER (PARTITION BY n.OrderNo, n.SubTotal ORDER BY n.SubTotal DESC) AS row_num " +
                              $"     FROM tbl_analytics n " +
                              $"        INNER JOIN [dbo].[tbl_location] l ON l.LocationCode = n.LocationId " +
                              $"        LEFT JOIN [dbo].[tbl_customer] c ON c.CustomerCode = n.CustomerId " +
                              $" ) a " +
                              $" WHERE  " +
                              $"     (CAST(a.TransactionDate AS DATE) = '{date.Date.ToString("yyyy-MM-dd")}' AND a.LocationId = {analyticsToDelete.storeId} AND a.CustomerId LIKE '%{memCodeLast6Digits}%' AND a.OrderNo LIKE '%{analyticsToDelete.jo}%' AND a.DeleteFlag = 0) " +
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

        public async Task<bool> UpdateAnalytics(UpdateAnalyticsDto updateAnalyticsDto)
        {
            var result = false;

            var GetAnalytics = await _dbContext.Analytics
                .Where(x => x.Id == updateAnalyticsDto.Id)
                .FirstOrDefaultAsync();

            if (GetAnalytics != null)
            {
                GetAnalytics.CustomerId = updateAnalyticsDto.CustomerId;
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

        public async Task<(string, bool, string, string)> GenerateA0File(GenerateA0FileDto generateA0FileDto)
        {
            try
            {
                var result = false;
                var fileName = "";

                var getInvoiceAnalytics = await GenerateInvoiceAnalytics(generateA0FileDto.analyticsParamsDto);
                var getAnalytics = await GetRawAnalytics(generateA0FileDto.analyticsParamsDto);
                var getInvoice = getInvoiceAnalytics.Item1;
                var isPending = getInvoiceAnalytics.Item2;

                if (isPending)
                {
                    return ("Please submit the analytics first and try again.", false, "", "");
                }
                
                var content = new StringBuilder();
                var formattedItems = getInvoice.Select(item =>
                {
                    var formattedTRXDate = FormatDate(item.HDR_TRX_DATE);
                    var formattedGLDate = FormatDate(item.HDR_GL_DATE);

                    return new
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
                });

                foreach (var item in formattedItems)
                {
                    fileName = item.FILENAME;
                    content.AppendLine($"{item.HDR_TRX_NUMBER}|{item.HDR_TRX_DATE}|{item.HDR_PAYMENT_TYPE}|{item.HDR_BRANCH_CODE}|{item.HDR_CUSTOMER_NUMBER}|{item.HDR_CUSTOMER_SITE}|{item.HDR_PAYMENT_TERM}|{item.HDR_BUSINESS_LINE}|{item.HDR_BATCH_SOURCE_NAME}|{item.HDR_GL_DATE}|{item.HDR_SOURCE_REFERENCE}|{item.DTL_LINE_DESC}|{item.DTL_QUANTITY}|{item.DTL_AMOUNT}|{item.DTL_VAT_CODE}|{item.DTL_CURRENCY}|{item.INVOICE_APPLIED}|{item.FILENAME}|");
                }

                // Write content to file
                string filePath = Path.Combine(generateA0FileDto.Path, fileName);
                File.WriteAllText(filePath, content.ToString());

                result = true;

                if (getAnalytics.Any())
                {
                    getAnalytics.ForEach(analyticsDto =>
                    {
                        analyticsDto.IsGenerate = true;
                    });

                    _dbContext.BulkUpdate(getAnalytics);
                    await _dbContext.SaveChangesAsync();
                }


                return ("Invoice Generated Successfully", result, content.ToString(), fileName);
            }
            catch (Exception)
            {

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

            DateTime date;
            if (DateTime.TryParse(analyticsParam.dates[0].ToString(), out date))
            {
                for (int i = 0; i < analyticsParam.memCode.Count(); i++)
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

                        _dbContext.Analytics.RemoveRange(analyticsToDelete);
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
                await DropTables(strStamp);
            }
            catch (Exception ex)
            {
                await DropTables(strStamp);
                throw;
            }
        }
    }
}
