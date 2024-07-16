using AutoMapper;
using CSI.Application.DTOs;
using CSI.Application.Interfaces;
using CSI.Domain.Entities;
using CSI.Infrastructure.Data;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Configuration;
using NetTopologySuite.Geometries;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CSI.Application.Services
{
    public class CategoryService : ICategoryService
    {
        private readonly AppDBContext _dbContext;

        public CategoryService(AppDBContext dBContext)
        {
            _dbContext = dBContext;
        }

        public async Task<List<Category>> GetCategory()
        {
            var getCategory = new List<Category>();
            getCategory = await _dbContext.Category.Where(c => c.DeleteFlag == false).ToListAsync();
            return getCategory;
        }
    }
}
