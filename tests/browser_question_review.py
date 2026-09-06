"""Optional browser check: python3 tests/browser_question_review.py (Playwright)."""
import json
from pathlib import Path
from playwright.sync_api import sync_playwright


# Synthetic options isolate browser behaviour from provider availability.
TEMPLATE = Path(__file__).resolve().parents[1] / 'evaluation' / 'question_review.html'
payload={'id':'browser-test','cards':[
 {'id':'one:1','context':{'topic':'Благодарность за сегодняшний день','stage':'next','messages':[{'role':'user','text':'Мы с мужем гуляли и ни о чём не говорили.'}]},'options':[{'alias':'first','model':'Model One','texts':['Что из сегодняшнего дня тебе хочется сохранить?']},{'alias':'second','model':'Model Two','texts':['Что в этом молчании стало для тебя тяжёлым?']}]},
 {'id':'series:1','context':{'topic':'Планы на завтра','stage':'next','messages':[]},'options':[{'alias':'second','model':'Model Two','texts':['Какие дела важны?','Какие дела важны?']},{'alias':'first','model':'Model One','texts':['Чего ты ждёшь от завтра?','</script><script>alert(1)</script>?']}]}
]}
Path('/tmp/question-review-browser-test.html').write_text(TEMPLATE.read_text().replace('__REVIEW_DATA__',json.dumps(payload,ensure_ascii=False).replace('<','\\u003c')))

with sync_playwright() as p:
    browser=p.chromium.launch(headless=True)
    page=browser.new_page(viewport={'width':1200,'height':900},accept_downloads=True)
    errors=[]
    page.on('pageerror',lambda e:errors.append(str(e)))
    page.goto('file:///tmp/question-review-browser-test.html')
    page.wait_for_load_state('networkidle')
    assert page.locator('.answer').count()==2
    assert 'Model One' not in page.locator('#options').inner_text()
    page.locator('input[type=checkbox]').first.check()
    page.locator('input[name=best][value=first]').check()
    page.locator('#note').fill('Первый вопрос хочется обдумать.')
    page.reload();page.wait_for_load_state('networkidle')
    assert page.locator('input[name=best][value=first]').is_checked()
    assert page.locator('#note').input_value()=='Первый вопрос хочется обдумать.'
    page.locator('#next').click()
    assert page.locator('.answer li').count()==4
    assert page.locator('#options').inner_text().count('</script>')==2
    page.locator('input[name=best][value=none]').check()
    with page.expect_download() as dl:
        page.locator('#export').click()
    path='/tmp/86cbejhh9-ratings-test.json';dl.value.save_as(path)
    saved=json.loads(Path(path).read_text())
    assert len(saved['ratings'])==2 and saved['models_revealed'] is False
    page.on('dialog',lambda d:d.accept())
    page.locator('#reveal').click()
    assert 'Model One' in page.locator('#options').inner_text()
    page.locator('#summary-panel summary').click()
    assert 'Все неудачные: 1' in page.locator('#summary').inner_text()
    page.locator('#reveal').click()
    with page.expect_download() as dl2:page.locator('#export').click()
    dl2.value.save_as('/tmp/86cbejhh9-ratings-revealed-test.json')
    assert json.loads(Path('/tmp/86cbejhh9-ratings-revealed-test.json').read_text())['models_revealed'] is True
    page.locator('#file').set_input_files(path)
    assert 'Сохранено' in page.locator('#saved').inner_text()
    bad=saved|{'comparison_id':'different'}
    Path('/tmp/86cbejhh9-wrong-ratings.json').write_text(json.dumps(bad))
    page.locator('#file').set_input_files('/tmp/86cbejhh9-wrong-ratings.json')
    assert 'другому сравнению' in page.locator('#saved').inner_text()
    page.locator('#prev').click()
    page.screenshot(path='/tmp/86cbejhh9-review-desktop.png',full_page=True)
    page.set_viewport_size({'width':390,'height':844})
    assert page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
    page.screenshot(path='/tmp/86cbejhh9-review-mobile.png',full_page=True)
    assert not errors,errors
    browser.close()
print('PASS: blind display, malicious text escaping, persistence, navigation, export/import, reveal, mobile overflow; no JS errors')
