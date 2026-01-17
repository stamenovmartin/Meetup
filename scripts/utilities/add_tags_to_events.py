#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Скрипта за автоматско додавање тагови на настани врз основа на нивните наслови.
"""

from main import create_app
from models.db_models import db, Event

# Мапирање на клучни зборови → тагови
TAG_MAPPING = {
    # IT тагови
    'ИТ Настан': ['IT', 'Technology', 'Networking'],
    'програмирање': ['Programming', 'IT', 'Development'],
    'хакатон': ['Hackathon', 'IT', 'Programming'],
    'стартап': ['Startup', 'Business', 'Entrepreneurship'],
    'wordpress': ['WordPress', 'IT', 'Web'],
    'docker': ['Docker', 'IT', 'DevOps'],
    'kubernetes': ['Kubernetes', 'IT', 'DevOps'],
    'код': ['Programming', 'IT'],
    'веб': ['Web', 'IT', 'Internet'],
    'апп': ['Apps', 'IT', 'Mobile'],
    'дигитален': ['Digital', 'Technology', 'IT'],
    'интернет': ['Internet', 'IT', 'Technology'],
    'софтвер': ['Software', 'IT', 'Development'],
    'game design': ['GameDev', 'IT', 'Design'],
    'видеоигри': ['GameDev', 'Gaming', 'IT'],

    # Музика/Концерти
    'концерт': ['Music', 'Concert', 'Entertainment'],
    'музика': ['Music', 'Entertainment'],
    'јаз': ['Jazz', 'Music', 'Concert'],
    'рок': ['Rock', 'Music', 'Concert'],
    'опера': ['Opera', 'Classical', 'Music'],
    'симфонија': ['Symphony', 'Classical', 'Music'],
    'оркестар': ['Orchestra', 'Classical', 'Music'],
    'солист': ['Solo', 'Classical', 'Music'],
    'виолина': ['Violin', 'Classical', 'Music'],
    'класик': ['Classical', 'Music'],
    'бенд': ['Band', 'Music', 'Concert'],

    # Театар/Драма
    'театар': ['Theater', 'Drama', 'Performance'],
    'драма': ['Drama', 'Theater', 'Performance'],
    'комедија': ['Comedy', 'Theater', 'Entertainment'],
    'претстава': ['Theater', 'Performance', 'Drama'],
    'балет': ['Ballet', 'Dance', 'Performance'],

    # Филм/Кино
    'филм': ['Film', 'Cinema', 'Entertainment'],
    'кино': ['Cinema', 'Film', 'Entertainment'],

    # Бизнис/Настани
    'конференција': ['Conference', 'Business', 'Networking'],
    'семинар': ['Seminar', 'Education', 'Workshop'],
    'работилница': ['Workshop', 'Education', 'Training'],
    'курс': ['Course', 'Education', 'Training'],
    'предавање': ['Lecture', 'Education', 'Academic'],
    'обука': ['Training', 'Education', 'Workshop'],

    # Спорт
    'спорт': ['Sports', 'Fitness', 'Active'],
    'фудбал': ['Football', 'Sports', 'Active'],
    'кошарка': ['Basketball', 'Sports', 'Active'],
    'маратон': ['Marathon', 'Sports', 'Running'],

    # Уметност/Култура
    'уметност': ['Art', 'Culture', 'Exhibition'],
    'изложба': ['Exhibition', 'Art', 'Culture'],
    'галерија': ['Gallery', 'Art', 'Culture'],
    'музеј': ['Museum', 'Culture', 'History'],

    # Забава
    'забава': ['Entertainment', 'Fun', 'Social'],
    'фестивал': ['Festival', 'Entertainment', 'Culture'],
    'партија': ['Party', 'Entertainment', 'Social'],
}

def auto_tag_event(event: Event) -> str:
    """Автоматски креирај тагови за настан врз основа на наслов и опис."""
    tags = set()

    # Проверка на наслов
    title = event.title.lower() if event.title else ''
    description = event.description.lower() if event.description else ''
    combined_text = f"{title} {description}"

    # Примени правила за мапирање
    for keyword, mapped_tags in TAG_MAPPING.items():
        if keyword.lower() in combined_text:
            tags.update(mapped_tags)

    # Ако нема тагови, стави генерички тагови врз основа на првиот збор
    if not tags:
        if 'настан' in title:
            tags.add('Event')
        if 'концерт' in title or 'сала' in title or 'музи' in title:
            tags.add('Music')
            tags.add('Entertainment')
        else:
            tags.add('Event')
            tags.add('General')

    return ','.join(sorted(tags))

def main():
    app = create_app()

    with app.app_context():
        events = Event.query.all()

        print(f'Процесирам {len(events)} настани...\n')

        updated = 0
        for event in events:
            # Ако настанот веќе има тагови, прескокни
            if event.tags and event.tags.strip():
                continue

            # Автоматски генерирај тагови
            auto_tags = auto_tag_event(event)
            event.tags = auto_tags
            updated += 1

            # Покажи напредок
            if updated % 50 == 0:
                print(f'Обработени {updated} настани...')

        # Зачувај промени
        db.session.commit()

        print(f'\n ГОТОВО!')
        print(f'Ажурирани {updated} настани со автоматски тагови.')

        # Покажи статистика
        with_tags = Event.query.filter(Event.tags != '').filter(Event.tags != None).count()
        print(f'\nСтатистика:')
        print(f'  - Вкупно настани: {len(events)}')
        print(f'  - Со тагови: {with_tags}')
        print(f'  - Без тагови: {len(events) - with_tags}')

        # Примери
        print('\nПримери на настани со нови тагови:')
        examples = Event.query.filter(Event.tags != '').limit(10).all()
        for e in examples[:5]:
            print(f'  • {e.title[:60]}')
            print(f'    Tags: {e.tags}')

if __name__ == '__main__':
    main()