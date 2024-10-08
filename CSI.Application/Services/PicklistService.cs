using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Services
{
    public class PicklistService
    {
        private readonly AppDBContext _dbContext;
        public PicklistService(AppDBContext dbContext) 
        {
            _dbContext = dbContext;
        }

        #region Get Departments
        public async Task<List<Department>> GetDepartmentCodes()
        {
            var result = new List<Department>();
            var list = await _dbContext.Departments.ToListAsync();
            foreach ( var item in list)
            {
                result.Add(item);
            }
            return result;
        }
        #endregion
    }
}
