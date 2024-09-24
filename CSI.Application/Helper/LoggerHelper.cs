using AutoMapper;
using CSI.Application.DTOs;
using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Helper
{
    public class LoggerHelper
    {
        private readonly AppDBContext _dbContext;
        private readonly IMapper _mapper;

        public LoggerHelper(AppDBContext dbContext,IMapper mapper)
        {
            _dbContext = dbContext;
            _mapper = mapper;
        }

        public async void Logger(string userId,string action,string exception,string username,string club)
        {
            var logsDto = new LogsDto();
            var logsMap = _mapper.Map<LogsDto, Logs>(logsDto);

            logsDto = new LogsDto
            {
                UserId = userId,
                Date = DateTime.Now,
                Action = action,
                Remarks = $"Error: {exception} - {username}",
                Club = club,
            };
            logsMap = _mapper.Map<LogsDto, Logs>(logsDto);
            _dbContext.Logs.Add(logsMap);
            await _dbContext.SaveChangesAsync();
        }
    }
}
