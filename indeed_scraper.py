#imports
from bs4 import BeautifulSoup as bs
from urllib.request import Request, urlopen
import pandas as pd
from multiprocessing import Pool
import time

class Indeed_Job_Scraper:
    def __init__(self):
        self.ineed_job_data = []
        self.visited = [1]
       
       
    def num_pages_visited(self):
        return len(self.visited)
    
    ####visit site
    def visit_site(self, job_search):
        page = Request('https://www.indeed.com{0}'.format(job_search),headers={'User-Agent': 'Mozilla/5.0'})
        page_cont = urlopen(page).read()
        soup = bs(page_cont,'html.parser')
        idx, pagination_dict = [],{}

        try:
            pagination = soup.find('ul', class_='pagination-list')
            pagination_url = list(set([p['href'] for p in pagination.find_all('a')]))
            pagination_dict = {int(p.split('&')[1].split('=')[1]):p for p in pagination_url if '&start=' in p and
                              int(p.split('&')[1].split('=')[1]) not in self.visited}
            idx = sorted(pagination_dict)
        except:
            pass
        return soup, idx, pagination_dict
    
    def parse_location(self, location_item):
        has_more_locations = False
        group_link = ''
        while hasattr(location_item,'has_attr'):
            ###check if the post is a part of a group. if it is, then visit the grou page          
            try:                
                link_to_more = location_item.find('a', 'more_loc')
                if link_to_more:                   
                    has_more_locations = True
                    group_link = link_to_more['href'].split('%26')[1]
            except:                
                pass            
            location_item = location_item.contents[0]
            return location_item, has_more_locations, group_link, link_to_more
    
    def save_job_description(self,job_url, job_id):
        
        soup, _ , _ = self.visit_site(job_url)
        
        job_description = soup.find('div', class_ = 'jobsearch-jobDescriptionText')
        output = open('job_descriptions\{0}.txt'.format(job_id),"w")
        try:
            output.write(str(job_description))
        except:
            print('err ', job_url)
        output.close()
        return
        
    def parse_and_log_result_content(self, result_item, parse_groups = True , page = '', group=''):
        has_more_locations = False

        result_item = bs(result_item, 'html.parser')

        
        try:
            job_id = result_item.a['id']
        except:
            job_id = False
            
        try:    
            job_description_url = result_item.a['href']
        except:
            job_description_url = "not available"
        
        try:
            salary_estimate = result_item.find('span', class_ = 'estimated-salary').svg['aria-label']
        except:
            salary_estimate = "not available"
            
        job_title = [jt.contents[0] for jt in  result_item.find_all('h2', class_ = 'jobTitle')[0].find_all('span') if jt.has_attr('title')]
        

        print(job_id , job_title)
        print(' ')
        
        company_name = result_item.find('span',class_ = 'companyName').contents[0]
        

        location, has_group_link, group_link, group_url = self.parse_location(
            result_item.find('div', class_ = "companyLocation")
        )

        #if has_group_link and parse_groups: self.run(group_url['href'])    


        company_url = 'NONE_GIVEN' 
        if hasattr(company_name,'has_attr'):            
            if company_name.has_attr('href'):              
                company_url = company_name['href']               
                company_name = company_name.contents[0]

        
        pay_load = {
                           'job_id': job_id,
                           'job_description_url': job_description_url,
                           'salary_estimate': salary_estimate,
                           'job_title': job_title[0],
                           'company': company_name, 
                           'company_url': company_url, 
                           'location': location,
                           'more_locations': has_group_link,
                           'group_to': group_link,
                           'group_from': group,
                           'page': page
                          }
        
        time.sleep(0.7)
        return pay_load

    def run(self, job_search):
        out = []
        page_num = job_search.split('&start=')[1]
        print(page_num)
        try:
            soup, pages_idx , pages = self.visit_site(job_search)
        except:
            return
        result_set = soup.find_all('a',class_="result")
        for r in result_set:
            data = self.parse_and_log_result_content(str(r), page = page_num)
            out.append(data)
            self.save_job_description('/viewjob?jk={0}'.format(data['job_id'].split('_')[1]), data['job_id'])
        return out
        
    def run_parallel(self, job_search: list ):
        
        if type(job_search) == list:
            n_procs = len(job_search)
            p = Pool(10)
            out = p.map(self.run, job_search)
            p.close()
            p.join()
        
        self.ineed_job_data.extend(out)
        
        flattened = []
        for j in self.ineed_job_data:
            if type(j) == list:
                for jj in j:
                    flattened.append(jj)
        self.ineed_job_data = flattened                 
        
        #if len(pages)>0 and pages_idx[0]<=1000: 
        #    self.visited.append(pages_idx[0])
        #    self.run(pages[pages_idx[0]])
        return out
    

