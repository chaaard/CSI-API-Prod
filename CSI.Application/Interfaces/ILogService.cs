using CSI.Application.DTOs;
using CSI.Domain.Entities;

namespace CSI.Application.Interfaces
{
    public interface ILogService
    {
        Task<(List<LogsDto>, int totalPages)> GetLogsListAsync(PaginationDto pagination);
        Task<Analytics> GetLogByIdAsync(string id);
    }
}
