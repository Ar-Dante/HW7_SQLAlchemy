from sqlalchemy import func, desc, select, and_

from database.models import Teacher, Student, Discipline, Grade, Group
from database.conn_to_db import session


def select_1():
    """
    Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
    SELECT s.fullname, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT 5;
    """
    result = session.query(
        Student.fullname,
        func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student) \
        .group_by(Student.id) \
        .order_by(desc('avg_grade')) \
        .limit(5).all()
    # order_by(Grade.grade.desc())
    return result


def select_2():
    """
    SELECT d.name, s.fullname, ROUND(AVG(g.grade), 2) as avg_grade
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    LEFT JOIN disciplines d ON d.id = g.discipline_id
    WHERE d.id = 5
    GROUP BY s.id
    ORDER BY avg_grade DESC
    LIMIT 1;
    """
    result = session.query(
        Discipline.name,
        Student.fullname,
        func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).join(Discipline) \
        .filter(Discipline.id == 5) \
        .group_by(Student.id, Discipline.name) \
        .order_by(desc('avg_grade')) \
        .limit(1).first()
    return result


def select_3():
    """ SELECT gr.name [Group],d.name Discipline , ROUND(AVG(g.grade), 2) as avg_grade
        FROM grades g
        JOIN [groups] gr ON gr.id = g.student_id
        JOIN disciplines d ON d.id = g.discipline_id
        WHERE d.id = 1 --Вказати, по якому id шукаємо предмет
        ORDER BY avg_grade DESC
        """
    result = session.query(
        Group.name.label("Group"),
        Discipline.name.label("Discipline"),
        func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).join(Group).join(Discipline) \
        .filter(Discipline.id == 1) \
        .group_by(Group.id, Discipline.name) \
        .order_by(desc('avg_grade')).all()
    return result


def select_4():
    """--Знайти середній бал на потоці (по всій таблиці оцінок).
        SELECT ROUND(AVG(g.grade), 2) as avg_grade
        FROM grades g
        """
    result = session.query(
        func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Student).all()
    return result


def select_5():
    """--Знайти які курси читає певний викладач
        SELECT t.fullname, d.name
        FROM disciplines d
        LEFT JOIN teachers t  ON t.id = d.teacher_id
        WHERE t.id = 2 --Вказати вчителя, по якому шукаємо дисципліну
        """
    result = session.query(
        Teacher.fullname.label("Teachers"),
        Discipline.name.label("Disciplines"))\
        .select_from(Discipline).join(Teacher) \
        .filter(Teacher.id == 2).all()
    return result


def select_6():
    """
    --Знайти список студентів у певній групі.
    SELECT s.fullname Student, g.name [Group]
    FROM groups g
    LEFT JOIN students s ON s.group_id  = g.id
    WHERE g.id = 1 --Вказати, по якій групі шукаємо студентів
    """
    result = session.query(
        Student.fullname,
        Group.name) \
        .select_from(Group).join(Student)\
        .filter(Group.id == 2).all()
    return result


def select_7():
    """
    --Знайти оцінки студентів у окремій групі з певного предмета.
    SELECT s.fullname Student, gr.name [Group], g.grade Grade, d.name Discipline
    FROM grades g
    LEFT JOIN students s ON s.id  = g.student_id
    LEFT JOIN disciplines d ON d.id = g.discipline_id
    LEFT JOIN groups  gr ON gr.id  = s.group_id
    WHERE gr.id = 1 AND d.id = 2 -- Вказати, по якому id шукати групу та предмет
    """
    result = session.query(
        Student.fullname,
        Group.name.label("Group"),
        Grade.grade,
        Discipline.name.label("Discipline")) \
        .select_from(Grade).join(Student).join(Group).join(Discipline) \
        .filter(and_(Group.id == 1,Discipline.id == 1)).all()
    return result


def select_8():
    """
    --Знайти середній бал, який ставить певний викладач зі своїх предметів.
    SELECT t.fullname Teacher, ROUND(AVG(g.grade), 2) average_grade
    FROM grades g
    JOIN disciplines d ON d.id = g.discipline_id
    JOIN teachers t ON t.id = d.teacher_id
    WHERE t.id = 4 -- Вказати, по якому id шукати вчителя
    """
    result = session.query(
        Teacher.fullname,
        func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Discipline).join(Teacher) \
        .filter(Teacher.id == 4)\
        .group_by(Teacher.fullname).all()
    return result


def select_9():
    """
    --Знайти список курсів, які відвідує студент
    SELECT s.fullname Student,d.name Discipline
    FROM disciplines d
    LEFT JOIN students s ON s.id = g.student_id
    LEFT JOIN grades g ON g.discipline_id = d.id
    WHERE s.id =2 --Вказати, по якому id шукати студента
    GROUP BY d.id """
    result = session.query(
        Student.fullname,
        Discipline.name) \
        .select_from(Grade).join(Discipline).join(Student) \
        .filter(Student.id == 1)\
        .group_by(Student.id, Discipline.name).all()
    return result


def select_10():
    """
    --Список курсів, які певному студенту читає певний викладач.
    SELECT s.fullname Student ,d.name Discipline, t.fullname Teacher
    FROM grades g
    LEFT JOIN students s ON s.id = g.student_id
    LEFT JOIN disciplines d ON d.id = g.discipline_id
    LEFT JOIN teachers t ON t.id = d.teacher_id
    WHERE s.id = 4 AND t.id = 4 --Вказати, по якому id шукати студента та вчителя
    GROUP BY d.id
    """
    result = session.query(
        Student.fullname,
        Discipline.name,
        Teacher.fullname) \
        .select_from(Grade).join(Discipline).join(Student).join(Teacher) \
        .filter(and_(Student.id == 2, Teacher.id == 4))\
        .group_by(Student.id, Discipline.name, Teacher.id).all()
    return result


def select_11():
    """--Середній бал, який певний викладач ставить певному студентові.
    SELECT t.fullname Teacher,s.fullname Student, ROUND(AVG(g.grade), 2) average_grade
    FROM grades g
    JOIN disciplines d ON d.id = g.discipline_id
    JOIN teachers t ON t.id = d.teacher_id
    JOIN students s ON s.id = g.student_id
    WHERE t.id = 2 AND s.id = 1 --Вкахати, по якому id шукати вчителя та студента
    """
    result = session.query(
        Teacher.fullname,
        Student.fullname,
        func.round(func.avg(Grade.grade), 2).label('avg_grade')) \
        .select_from(Grade).join(Discipline).join(Student).join(Teacher) \
        .filter(and_(Student.id == 2, Teacher.id == 4))\
        .group_by(Student.id, Teacher.id).all()
    return result

def select_12():
    """
    -- Оцінки студентів у певній групі з певного предмета на останньому занятті.
    select s.id, s.fullname, g.grade, g.date_of
    from grades g
    join students s on s.id = g.student_id
    where g.discipline_id = 3 and s.group_id = 3 and g.date_of = (
        select max(date_of)
        from grades g2
        join students s2 on s2.id = g2.student_id
        where g2.discipline_id = 3 and s2.group_id = 3
    );
    :return:
    """
    subquery = (select(func.max(Grade.date_of)).join(Student).filter(and_(
        Grade.discipline_id == 3, Student.group_id == 3
    )).scalar_subquery())

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.date_of) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(
        Grade.discipline_id == 3, Student.group_id == 3, Grade.date_of == subquery
    )).all()
    return result


if __name__ == '__main__':
    print(select_11())
