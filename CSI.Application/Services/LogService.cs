using AutoMapper;
using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Services
{
    public class LogService : ILogService
    {
        private readonly AppDBContext _dbContext;
        private readonly IJwtService _jwtService;
        private readonly IMapper _mapper;

        public LogService(AppDBContext dbContext, IJwtService jwtService, IMapper mapper)
        {
            _dbContext = dbContext;
            _jwtService = jwtService;
            _mapper = mapper;
        }

        public async Task<(List<LogsDto>, int totalPages)> GetLogsListAsync(PaginationDto pagination)
        {
            var query = _dbContext.Logs
                .Join(_dbContext.Users, x => x.UserId, y => y.Id.ToString(), (x, y) => new { x, y })
                .Join(_dbContext.Locations, xy => xy.x.Club, z => z.LocationCode.ToString(), (xy, z) => new { xy.x, xy.y, z })
                .Select(n => new LogsDto
                {
                    UserId = n.y.FirstName + " " + n.y.LastName,
                    Date = n.x.Date,
                    Action = n.x.Action,
                    Remarks = n.x.Remarks,
                    RowsCountBefore = n.x.RowsCountBefore,
                    RowsCountAfter = n.x.RowsCountAfter,
                    TotalAmount = n.x.TotalAmount,
                    Club = n.z.LocationName.Replace("KAREILA", n.x.Club.ToString()).Trim(),
                    CustomerId = n.x.CustomerId,
                    Filename = n.x.Filename,
                    ActionId = n.x.ActionId,
                    AnalyticsId = n.x.AnalyticsId,
                    AdjustmentId = n.x.AdjustmentId
                })
                .AsQueryable();

            // Searching
            if (!string.IsNullOrEmpty(pagination.SearchQuery))
            {
                var searchQuery = $"%{pagination.SearchQuery.ToLower()}%";

                query = query.Where(c =>
                    //Add the category column here
                    (EF.Functions.Like(c.UserId.ToLower(), searchQuery)) ||
                    (EF.Functions.Like(c.Action.ToLower(), searchQuery)) ||
                    (EF.Functions.Like(c.Remarks.ToLower(), searchQuery))
                );
            }

            var totalItemCount = await query.CountAsync();
            var totalPages = (int)Math.Ceiling((double)totalItemCount / pagination.PageSize);

            var logList = await query
                .Skip((pagination.PageNumber - 1) * pagination.PageSize)
                .Take(pagination.PageSize)
                .ToListAsync();

            return (logList, totalPages);
        }

        public async Task<Analytics> GetLogByIdAsync(string id)
        {
            var getLog = new Analytics();
            getLog = await _dbContext.Analytics.Where(x => x.Id == Convert.ToInt32(id)).FirstAsync();
            if (getLog == null)
            {
                return new Analytics();
            }
            return getLog;
        }
    }
}
