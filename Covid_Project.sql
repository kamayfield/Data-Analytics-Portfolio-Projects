/*
Queries used for Tableau Project
*/


Select Location, date, total_cases, new_cases, total_deaths, population
From CovidProject.dbo.CovidDeaths
where continent is not null
order by 1,2

-- Looking at Total Cases vs Total Deaths
-- Shows likelihood of dying if you contract covid in your country

Select Location, date, total_deaths, total_cases, (total_deaths/total_cases)*100 as DeathPercentage
From CovidProject.dbo.CovidDeaths
where location like '%states%'
and continent is not null
order by 1,2

-- Looking at Total Cases vs Population
-- Shows what percentage of population contracted covid

Select Location, date, total_cases, population, (total_cases/population)*100 as ContractionPercentage
From CovidProject.dbo.CovidDeaths
where location like '%states%'
and continent is not null
order by 1,2

-- Looking at countries with highest infection rate compared to population

Select Location, population, MAX(total_cases) as HighestInfectionCount, MAX((total_cases/population))*100 as InfectionPercentage
From CovidProject.dbo.CovidDeaths
--where location like '%states%'
where continent is not null
group by Location, population
order by InfectionPercentage desc

-- Showing countries with highest death count per population

Select Location, MAX(cast(total_deaths as int)) as TotalDeathCount
From CovidProject.dbo.CovidDeaths
--where location like '%states%'
where continent is not null
group by Location
order by TotalDeathCount desc

-- Showing continents with highest death count per population

Select location, MAX(cast(total_deaths as int)) as TotalDeathCount
From CovidProject.dbo.CovidDeaths
--where location like '%states%'
where continent is null
group by location
order by TotalDeathCount desc

-- Global numbers

Select SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as DeathPercentage
From CovidProject..CovidDeaths
--Where location like '%states%'
where continent is not null 
--Group By date
order by 1,2

-- Looking at Total Population vs Vaccinations

Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(cast(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location,
	dea.date) as RollingPeopleVaccinated
from CovidProject..CovidDeaths dea
join CovidProject..CovidVaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
order by 2, 3

-- CTE

with PopvsVac (continent, location, date, population, new_vaccinations, RollingPeopleVaccinated)
as
(
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(cast(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location,
	dea.date) as RollingPeopleVaccinated
from CovidProject..CovidDeaths dea
join CovidProject..CovidVaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
--order by 2, 3
)

Select *, (RollingPeopleVaccinated/Population)*100
From PopvsVac

-- Temp Table

drop table if exists #PercentPopulationVaccinated
create table #PercentPopulationVaccinated
(
continent nvarchar(255),
location nvarchar(255),
date datetime,
population numeric,
new_vaccinations numeric,
RollingPeopleVaccinated numeric
)

insert into #PercentPopulationVaccinated
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(cast(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location,
	dea.date) as RollingPeopleVaccinated
from CovidProject..CovidDeaths dea
join CovidProject..CovidVaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
--order by 2, 3

Select *, (RollingPeopleVaccinated/Population)*100
From #PercentPopulationVaccinated

-- Creating view to store data for later visualizations

create view PercentPopulationVaccinated as
Select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations
, SUM(cast(vac.new_vaccinations as int)) over (partition by dea.location order by dea.location,
	dea.date) as RollingPeopleVaccinated
from CovidProject..CovidDeaths dea
join CovidProject..CovidVaccinations vac
	on dea.location = vac.location
	and dea.date = vac.date
where dea.continent is not null
--order by 2, 3
