from time import sleep

from bs4 import BeautifulSoup
import requests
import weasyprint
import pandas as pd
import pathlib
import os


def get_articles(keyword):
    urls = []

    html_full = requests.get('https://www.forbes.com/search/?q={}'.format(keyword)).content
    soup_full = BeautifulSoup(html_full, 'html.parser')
    num_articles = soup_full.find('div', attrs={'class': 'search-heading'}).attrs['count']
    print('Achei {} artigos contendo \"{}\"'.format(num_articles, keyword))

    start = 0
    while start < int(num_articles):
        print('Fetching articles {} to {}'.format(start, start + 19))
        html_simple = requests.get(
            'https://www.forbes.com/simple-data/search/more/?start={}&q={}'.format(start, keyword)).content
        soup_simple = BeautifulSoup(html_simple, 'html.parser')
        article_links = soup_simple.find_all('a', attrs={'class': 'stream-item__title'})
        for article in article_links:
            if len(article.contents) >= 1:
                urls.append(article.attrs['href'])
        start += 20

    return urls


def fetch_articles(keyword):
    path = os.path.join('tmp', 'article_urls')
    pathlib.Path(path).mkdir(parents=True, exist_ok=True)
    file_name = os.path.join(path, keyword + '.csv')
    if os.path.exists(file_name):
        print('{} exists, skipping...'.format(file_name))
        return
    article_urls = get_articles(keyword)
    df = pd.DataFrame({'url': article_urls}, dtype=str)
    df.to_csv(os.path.join(path, keyword + '.csv'), index=False)


def read_article_urls_from_csv(keyword):
    path = os.path.join('tmp', 'article_urls', keyword + '.csv')
    df = pd.read_csv(path)
    return df.loc[df.url.str.startswith('https://www.forbes.com/sites/')]


def get_pdfs():
    keywords = ['employees dress', 'professional dress', 'workplace dress', 'office dress', 'dress at work',
                'business dress' 'employees dress code', 'professional dress code', 'workplace dress code',
                'office dress code', 'dress code at work', 'business dress code', 'employees uniforms',
                'professional uniforms', 'workplace uniforms', 'office uniforms', 'uniforms at work',
                'business uniforms', 'employees attire', 'professional attire', 'workplace attire', 'office attire',
                'attire at work', 'business attire', 'employees clothes', 'professional clothes', 'workplace clothes',
                'office clothes', 'clothes at work', 'business clothes']

    for keyword in keywords:
        fetch_articles(keyword)
        processed_file = os.path.join('tmp', 'processed.csv')
        if os.path.isfile(processed_file):
            processed_df = pd.read_csv(processed_file)
        else:
            processed_df = pd.DataFrame(columns=['year', 'title', 'url'])
        try:
            urls_df = read_article_urls_from_csv(keyword)
            for i, row in urls_df.iterrows():
                if not processed_df.loc[processed_df['url'] == row['url']].empty:
                    print('Url {} já foi processada, pulando'.format(row['url']))
                    continue

                html = requests.get(row['url']).content
                soup = BeautifulSoup(html, 'html.parser')
                title = ''
                for div in soup.findAll('h1', attrs={'class': 'fs-headline'}):
                    if not div:
                        continue
                    title = div.contents[0]
                    break

                year = '1900'
                for div in soup.findAll('div', attrs={'class': 'content-data'}):
                    if not div.find('time'):
                        continue

                    year = div.find('time').contents[0].split(' ')[2]
                    year = year[:-1]  # drop the ',' at the end
                    break

                picture = ''
                for div in soup.findAll('figure', attrs={'class': 'embed-base'}):
                    if not div.find('img'):
                        continue

                    picture = div.find('img').attrs['src']
                    break

                title = title.replace('/', '_')
                print("Convertendo para pdf: {}".format(title))
                pdf = weasyprint.HTML(row['url']).write_pdf()
                # keyword = keyword.replace('%20', ' ')
                out_dir = 'out/' + keyword + '/' + year;
                pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
                out_file = os.path.join(out_dir, title + '.pdf')
                open(out_file, 'wb').write(pdf)
                print(i, year, title, picture)
                processed_df = processed_df.append({'year': year, 'title': title.replace(',', ''), 'url': row['url']},
                                                   ignore_index=True)
        finally:
            processed_df.to_csv(processed_file, index=False)


def generate_processed():
    final_df = pd.DataFrame(columns=['year', 'title', 'keyword', 'url'])
    processed_df = pd.read_csv('tmp/processed.csv')
    processed_df = processed_df.drop_duplicates(subset=['url'], keep='first')
    processed_df = processed_df.set_index('url')
    for file in os.listdir('tmp/article_urls'):
        urls_df = pd.read_csv('tmp/article_urls/' + file)
        keyword = file.split('.')[0]
        urls_df['keyword'] = keyword
        urls_df = urls_df.set_index('url')
        urls_df['url'] = urls_df.index
        result = pd.concat([processed_df, urls_df], axis=1, join='inner')
        final_df = final_df.append(result)

    return final_df


if __name__ == '__main__':
    # get_pdfs()
    df = generate_processed()
    cols = ['keyword', 'year', 'title', 'url']
    df = df[cols]
    df = df.sort_values(by=['keyword', 'year'])
    df = df.drop_duplicates(subset=['url'], keep='first')
    df.to_excel('artigos.xlsx', engine='xlsxwriter', index=False)


