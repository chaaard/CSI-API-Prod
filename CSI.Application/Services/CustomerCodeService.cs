
using AutoMapper;
using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query.SqlExpressions;
using Microsoft.Extensions.Configuration;
using Microsoft.IdentityModel.Tokens;
using System;
using System.Text.RegularExpressions;
using static Microsoft.EntityFrameworkCore.DbLoggerCategory;

namespace CSI.Application.Services
{
    public class CustomerCodeService : ICustomerCodeService
    {
        private readonly AppDBContext _dbContext;
        private readonly IConfiguration _configuration;
        private readonly IMapper _mapper;

        public CustomerCodeService(IConfiguration configuration, AppDBContext dBContext, IMapper mapper)
        {
            _configuration = configuration;
            _dbContext = dBContext;
            _mapper = mapper;
        }

        public async Task<(List<CustomerCodeDto>, int totalPages)> GetCustomerCodesAsync(PaginationDto pagination)
        {
            var query = _dbContext.CustomerCodes
                .GroupJoin(
                    _dbContext.Category,
                    customerCode => customerCode.CategoryId,
                    category => category.Id,
                    (customerCode, categories) => new { CustomerCode = customerCode, Categories = categories })
                .SelectMany(
                    x => x.Categories.DefaultIfEmpty(), // this ensures a left join
                    (x, category) => new CustomerCodeDto
                    {
                        Id = x.CustomerCode.Id,
                        CustomerNo = x.CustomerCode.CustomerNo,
                        CustomerCode = x.CustomerCode.CustomerCode,
                        CustomerName = x.CustomerCode.CustomerName,
                        CategoryName = category != null ? category.CategoryName : null,
                        CategoryId = category.Id != null ? category.Id : 0,
                        DeleteFlag = x.CustomerCode.DeleteFlag,
                    })
                .AsQueryable();

            // Searching
            if (!string.IsNullOrEmpty(pagination.SearchQuery))
            {
                var searchQuery = $"%{pagination.SearchQuery.ToLower()}%";

                query = query.Where(c =>
                    (EF.Functions.Like(c.CustomerNo.ToLower(), searchQuery)) ||
                    (EF.Functions.Like(c.CustomerName.ToLower(), searchQuery)) ||
                    (EF.Functions.Like(c.CustomerCode.ToLower(), searchQuery))
                //Add the category column here
                );
            }

            // Sorting
            if (!string.IsNullOrEmpty(pagination.ColumnToSort))
            {
                var sortOrder = pagination.OrderBy == "desc" ? "desc" : "asc";

                switch (pagination.ColumnToSort.ToLower())
                {
                    case "customername":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.CustomerName) : query.OrderByDescending(c => c.CustomerName);
                        break;
                    case "customercode":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.CustomerCode) : query.OrderByDescending(c => c.CustomerCode);
                        break;
                    case "customerno":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.CustomerNo) : query.OrderByDescending(c => c.CustomerNo);
                        break;
                    case "deleteflag":
                        query = sortOrder == "asc" ? query.OrderBy(c => c.DeleteFlag) : query.OrderByDescending(c => c.DeleteFlag);
                        break;
                    //Another case here for category
                    default:
                        break;
                }
            }

            var totalItemCount = await query.CountAsync();
            var totalPages = (int)Math.Ceiling((double)totalItemCount / pagination.PageSize);

            var customerCodesList = await query
                .Skip((pagination.PageNumber - 1) * pagination.PageSize)
                .Take(pagination.PageSize)
                .ToListAsync();

            return (customerCodesList, totalPages);
        }






        public async Task<List<CustomerCodes>> GetCustomerDdCodesAsync()
        {
            var query = await _dbContext.CustomerCodes
                .ToListAsync();

            return query;
        }

        public async Task<CustomerCodes> GetCustomerCodeByIdAsync(int Id)
        {
            var getCustomerCodes = new CustomerCodes();
            getCustomerCodes = await _dbContext.CustomerCodes.Where(x => x.Id == Id).FirstAsync();
            return getCustomerCodes;
        }

        public async Task<CustomerCodes> UpdateCustomerCodeByIdAsync(CustomerCodeParamsDto customerCode)
        {
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var getCustomerCode = await _dbContext.CustomerCodes.SingleOrDefaultAsync(x => x.Id == customerCode.Id);

                if (getCustomerCode != null)
                {
                    var oldCustomerName = getCustomerCode.CustomerName;
                    var oldCustomerCode = getCustomerCode.CustomerCode;
                    var oldCustomerNo = getCustomerCode.CustomerNo;
                    var oldDeleteFlag = getCustomerCode.DeleteFlag;
                    var oldCategoryId = getCustomerCode.CategoryId;
                    getCustomerCode.CustomerName = customerCode.CustomerName;
                    getCustomerCode.CustomerCode = customerCode.CustomerCode;
                    getCustomerCode.CustomerNo = customerCode.CustomerNo;
                    getCustomerCode.DeleteFlag = customerCode.DeleteFlag;
                    getCustomerCode.CategoryId = customerCode.CategoryId;
                    await _dbContext.SaveChangesAsync();

                    logsDto = new LogsDto
                    {
                        UserId = customerCode.UserId,
                        Date = DateTime.Now,
                        Action = "Update Merchant",
                        Remarks = $"Updated Successfully" +
                                  $"Id: {customerCode.Id} : " +
                                  $"CustomerName: {oldCustomerName} -> {customerCode.CustomerName}, " +
                                  $"CustomerCode: {oldCustomerCode} -> {customerCode.CustomerCode}, " +
                                  $"CustomerNo: {oldCustomerNo} -> {customerCode.CustomerNo}, " +
                                  $"CategoryId: {oldCategoryId} -> {customerCode.CategoryId}, " +
                                  $"DeleteFlag: {oldDeleteFlag} -> {customerCode.DeleteFlag}"
                    };
                    logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                    _dbContext.Logs.Add(logsMap);
                    await _dbContext.SaveChangesAsync();

                    return getCustomerCode;
                }
                else
                {
                    return new CustomerCodes();
                }
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    UserId = customerCode.UserId,
                    Date = DateTime.Now,
                    Action = "Update Merchant",
                    Remarks = $"Error: {ex.Message}",
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<CustomerCodes> InsertCustomerCodeAsync(CustomerCodeParamsDto customerCodeParams)
        {
            var logsDto = new LogsDto();
            var logsMap = new Logs();
            try
            {
                var getCustomerCode = await _dbContext.CustomerCodes
                    .SingleOrDefaultAsync(x => x.CustomerCode == customerCodeParams.CustomerCode 
                                            || x.CustomerNo == customerCodeParams.CustomerNo
                                            || x.CustomerName == customerCodeParams.CustomerName);

                if (getCustomerCode == null)
                {
                    // Define a lambda expression for inserting the user
                    Func<CustomerCodeParamsDto, Task<CustomerCodes>> insertCustomerLambda = async newMerchant =>
                    {
                        var getCustomer = new CustomerCodes
                        {
                            CustomerNo = newMerchant.CustomerNo,
                            CustomerName = newMerchant.CustomerName,
                            CustomerCode = newMerchant.CustomerCode,
                            CategoryId = newMerchant.CategoryId,
                            DeleteFlag = newMerchant.DeleteFlag,
                        };

                        await _dbContext.CustomerCodes.AddAsync(getCustomer);
                        await _dbContext.SaveChangesAsync(); // Ensure changes are saved to the database

                        var newCustomerId = getCustomer.Id;

                        logsDto = new LogsDto
                        {
                            UserId = customerCodeParams.UserId,
                            Date = DateTime.Now,
                            Action = "Update Merchant",
                            Remarks = $"Saved Successfully" +
                                $"Id: {newCustomerId} : "
                        };
                        logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                        _dbContext.Logs.Add(logsMap);
                        await _dbContext.SaveChangesAsync();

                        return getCustomer;
                    };

                    // Invoke the lambda to insert the customer
                    return await insertCustomerLambda(customerCodeParams);
                }
                return null;
            }
            catch (Exception ex)
            {
                logsDto = new LogsDto
                {
                    Date = DateTime.Now,
                    Action = "Insert Merchant",
                    Remarks = $"Error: {ex.Message}",
                };
                logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
                _dbContext.Logs.Add(logsMap);
                await _dbContext.SaveChangesAsync();
                throw;
            }
        }

        public async Task<bool> DeleteCustomerCodeByIdAsync(int Id)
        {
            var getCustomerCode = await _dbContext.CustomerCodes.SingleOrDefaultAsync(x => x.Id == Id);

            if (getCustomerCode != null)
            {
                getCustomerCode.DeleteFlag = true;
                await _dbContext.SaveChangesAsync();

                return true;
            }
            else
            {
                return false;
            }
        }
        public async Task<List<CustomerCodeDto>> GetCustomerCodesByCategory(PaginationDto pagination)
        {
             if (pagination.ByMerchant)
            {
                var result = new List<CustomerCodeDto>();

                if (pagination.CategoryId == null || pagination.CategoryId == 0)
                {
                    var query = _dbContext.CustomerCodes
                        .Where(customerCode => customerCode.DeleteFlag == false)
                        //.GroupJoin(_dbContext.Category, x => x.CategoryId, y => y.Id, (x, y) => new { x, y })
                        //.SelectMany(
                        //    xy => xy.y.DefaultIfEmpty(),
                        //    (xy, y) => new { xy.x, y }
                        //)
                        .Select(n => new CustomerCodeDto
                        {
                            Id = n.Id,
                            CustomerNo = n.CustomerNo,
                            CustomerCode = n.CustomerCode,
                            CustomerName = n.CustomerName,
                            DeleteFlag = n.DeleteFlag,
                            CategoryId = n.CategoryId,
                        })
                        //.Where(c => c.CategoryId == pagination.CategoryId)
                        .OrderBy(n => n.CustomerName)
                        .AsQueryable();

                        result = query.ToList();

                        return (result);
                }
                else {
                    var query = _dbContext.CustomerCodes
                        .Where(customerCode => customerCode.DeleteFlag == false)
                        //.GroupJoin(_dbContext.Category, x => x.CategoryId, y => y.Id, (x, y) => new { x, y })
                        //.SelectMany(
                        //    xy => xy.y.DefaultIfEmpty(),
                        //    (xy, y) => new { xy.x, y }
                        //)
                        .Select(n => new CustomerCodeDto
                        {
                        Id = n.Id,
                        CustomerNo = n.CustomerNo,
                        CustomerCode = n.CustomerCode,
                        CustomerName = n.CustomerName,
                        DeleteFlag = n.DeleteFlag,
                        CategoryId = n.CategoryId,
                        })
                        .Where(c => c.CategoryId == pagination.CategoryId)
                        .OrderBy(n => n.CustomerName)
                        .AsQueryable();

                        result = query.ToList();

                        return (result);
                }

                
            }
            else
            {
                var result = new List<CustomerCodeDto>();
                int isVisible = 1;
                if (pagination.IsVisible != null) {
                    if (pagination.IsVisible == false)
                    {
                        isVisible = 0;
                    }
                }

                try {
                    if (pagination.IsAllVisible != null)
                    {
                        if (pagination.IsAllVisible == false)
                        {
                            var query = await _dbContext.CategoryCode
                           .FromSqlRaw($@"SELECT temp3.[CategoryId], temp3.[CustomerCodes], temp4.[CategoryName], 1 as [IsVisible]" +
                               "FROM ( " +
                                   "SELECT " +
                                       "   [CategoryId], " +
                                       "   CASE " +
                                       "       WHEN COUNT([CustomerCode]) > 1 THEN " +
                                       "           STUFF((" +
                                       "               SELECT ',' + CONVERT(VARCHAR(MAX), [CustomerCode])  " +
                                       "                FROM [CSI.Development].[dbo].[tbl_customer] AS temp2 " +
                                       "                WHERE temp2.[CategoryId] = temp1.[CategoryId] " +
                                       "                FOR XML PATH(''), TYPE " +
                                       "            ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') " +
                                       "       ELSE   CONVERT(VARCHAR(MAX), MAX([CustomerCode])) " +
                                       "   END AS [CustomerCodes] " +
                                   "FROM[CSI.Development].[dbo].[tbl_customer] AS temp1 " +
                                   "WHERE temp1.[DeleteFlag] = 0 " +
                                   "GROUP BY[CategoryId] " +
                               ") AS temp3 " +
                               "LEFT JOIN[CSI.Development].[dbo].[tbl_category] AS temp4 ON temp3.[CategoryId] = temp4.[Id] WHERE temp4.[IsVisible] = " + isVisible.ToString()).ToListAsync();

                            result = query.Select(n => new CustomerCodeDto
                            {
                                CategoryId = n.CategoryId,
                                CustomerCodes = ConvertCommaSeparatedStringToList(n.CustomerCodes),
                                CategoryName = n.CategoryName,
                                IsVisible = Convert.ToBoolean(n.IsVisible)
                            }).ToList();
                            return (result);
                        }
                        else
                        {
                            var query = await _dbContext.CategoryCode
                                .FromSqlRaw($@"(Select 0 as [CategoryId], (SELECT STUFF((SELECT ', ' + CustomerCode " +
                                             "FROM [CSI.Development].[dbo].[tbl_customer] WHERE DeleteFlag = 0" +
                                             "FOR XML PATH(''), TYPE " +
                                                ").value('.', 'NVARCHAR(MAX)'), 1, 2, ''))  AS [CustomerCodes], 'ALL' as [CategoryName], 1 as [IsVisible]) " +
                                                "UNION " +
                                                "SELECT temp3.[CategoryId], temp3.[CustomerCodes], temp4.[CategoryName], temp4.[IsVisible] " +
                                                "FROM ( " +
                                                    "SELECT " +
                                                        "   [CategoryId], " +
                                                        "   CASE " +
                                                        "       WHEN COUNT([CustomerCode]) > 1 THEN " +
                                                        "           STUFF((" +
                                                        "               SELECT ',' + CONVERT(VARCHAR(MAX), [CustomerCode])  " +
                                                        "                FROM [CSI.Development].[dbo].[tbl_customer] AS temp2 " +
                                                        "                WHERE temp2.[CategoryId] = temp1.[CategoryId] " +
                                                        "                FOR XML PATH(''), TYPE " +
                                                        "            ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') " +
                                                        "       ELSE   CONVERT(VARCHAR(MAX), MAX([CustomerCode])) " +
                                                        "   END AS [CustomerCodes] " +
                                                    "FROM[CSI.Development].[dbo].[tbl_customer] AS temp1 " +
                                                    "WHERE temp1.[DeleteFlag] = 0 " +
                                                    "GROUP BY[CategoryId] " +
                                                ") AS temp3 " +
                                                "LEFT JOIN[CSI.Development].[dbo].[tbl_category] AS temp4 ON temp3.[CategoryId] = temp4.[Id] WHERE temp4.[IsVisible] = " + isVisible.ToString())
                                            .ToListAsync();

                            result = query.Select(n => new CustomerCodeDto
                            {
                                CategoryId = n.CategoryId,
                                CustomerCodes = ConvertCommaSeparatedStringToList(n.CustomerCodes),
                                CategoryName = n.CategoryName,
                                IsVisible = Convert.ToBoolean(n.IsVisible)
                            }).ToList();
                            return (result);
                        }
                    }
                    else
                    {

                        var query = await _dbContext.CategoryCode
                            .FromSqlRaw($@"SELECT temp3.[CategoryId], temp3.[CustomerCodes], temp4.[CategoryName], 1 as [IsVisible] " +
                                "FROM ( " +
                                    "SELECT " +
                                        "   [CategoryId], " +
                                        "   CASE " +
                                        "       WHEN COUNT([CustomerCode]) > 1 THEN " +
                                        "           STUFF((" +
                                        "               SELECT ',' + CONVERT(VARCHAR(MAX), [CustomerCode])  " +
                                        "                FROM [CSI.Development].[dbo].[tbl_customer] AS temp2 " +
                                        "                WHERE temp2.[CategoryId] = temp1.[CategoryId] " +
                                        "                FOR XML PATH(''), TYPE " +
                                        "            ).value('.', 'NVARCHAR(MAX)'), 1, 1, '') " +
                                        "       ELSE   CONVERT(VARCHAR(MAX), MAX([CustomerCode])) " +
                                        "   END AS [CustomerCodes] " +
                                    "FROM[CSI.Development].[dbo].[tbl_customer] AS temp1 " +
                                    "WHERE temp1.[DeleteFlag] = 0 " +
                                    "GROUP BY[CategoryId] " +
                                ") AS temp3 " +
                                "LEFT JOIN[CSI.Development].[dbo].[tbl_category] AS temp4 ON temp3.[CategoryId] = temp4.[Id] WHERE temp4.[IsVisible] = " + isVisible.ToString()).ToListAsync();

                        result = query.Select(n => new CustomerCodeDto
                        {
                            CategoryId = n.CategoryId,
                            CustomerCodes = ConvertCommaSeparatedStringToList(n.CustomerCodes),
                            CategoryName = n.CategoryName,
                            IsVisible = Convert.ToBoolean(n.IsVisible)
                        }).ToList();
                        return (result);

                    }

                }

                catch (Exception ex) {
                    string test = ex.Message;
                    return (null);
                }
                

            }
        }

        public static List<string> ConvertCommaSeparatedStringToList(string input)
        {
            if (string.IsNullOrEmpty(input))
            {
                return new List<string>();
            }

            string[] splitStrings = input.Split(new[] { "," }, StringSplitOptions.None);

            if (splitStrings.Length > 0)
            {
                splitStrings[0] = splitStrings[0].TrimStart(',');
                splitStrings[splitStrings.Length - 1] = splitStrings[splitStrings.Length - 1].TrimEnd(',');
            }

            return splitStrings.ToList();
        }
    }
}
