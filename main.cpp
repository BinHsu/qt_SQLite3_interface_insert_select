#include <iostream>
#include <QCoreApplication>
#include <QtCore>
#include <QtSql/QSqlDatabase>
#include <QtSql/QSqlError>
#include <QString>
#include <QElapsedTimer>

#include "task.h"
#include "sqlite3.h"

static QString CONNECTION_NAME("metadata");

Task::Task(QObject *parent)
    : QObject(parent)
{}

void Task::run()
{
    QElapsedTimer timer;
    timer.start();
    //connect DB
    sqlite3 *db(nullptr);
    QString dbname("metadata.db");
    bool success(true);
    int result = sqlite3_open(dbname.toStdString().c_str(), &db);
    if (SQLITE_OK != result)
    {
        sqlite3_close(db);
        qDebug() << __FUNCTION__ << " - open DB: " << dbname << " failed: " << sqlite3_errstr(result) << endl;
        emit finished();
    }

    QString transaction("BEGIN TRANSACTION");
    result = sqlite3_exec(db, transaction.toStdString().c_str(), nullptr, nullptr, nullptr);
    if (SQLITE_OK != result)
    {
        sqlite3_close(db);
        qDebug() << __FUNCTION__ << " - begin transaction failed: " << sqlite3_errstr(result) << endl;
        emit finished();
    }

    auto pStrInsSql = "INSERT INTO metadata2(time, value) VALUES (?,?)";
    sqlite3_stmt *pInsertStmt(nullptr);
    result = sqlite3_prepare_v2(db, pStrInsSql, strlen(pStrInsSql), &pInsertStmt, nullptr);
    if (SQLITE_OK != result)
    {
        sqlite3_close(db);
        qDebug() << __FUNCTION__ << " - prepare stmt failed: " << sqlite3_errstr(result) << endl;
        emit finished();
    }

    for (int i(0); i < 100; ++i)
    {
        qint8 *value(reinterpret_cast<qint8*>(&i));//[] = {i};
        result = sqlite3_bind_blob(pInsertStmt, 2, value, sizeof(i), SQLITE_TRANSIENT);
        if (SQLITE_OK != result)
        {
            qDebug() << __FUNCTION__ << " - bind blob failed: " << sqlite3_errstr(result) << endl;
            success = false;
            break;
        }
        qint64 time(i);
        result = sqlite3_bind_int64(pInsertStmt, 1, time);
        if (SQLITE_OK != result)
        {
            qDebug() <<__FUNCTION__ << " - bind int64 failed: " << sqlite3_errstr(result) << endl;
            success = false;
            break;
        }

        result = sqlite3_step(pInsertStmt);
        if (SQLITE_DONE != result)
        {
            qDebug() << __FUNCTION__ << " - exec failed: " << sqlite3_errstr(result) << endl;
            success = false;
            break;
        }

        sqlite3_clear_bindings(pInsertStmt);
        sqlite3_reset(pInsertStmt);
    }

    if (!success)
    {
        QString transaction("ROLLBACK");
        sqlite3_exec(db, transaction.toStdString().c_str(), nullptr, nullptr, nullptr);
    }
    else
    {
        QString transaction("COMMIT");
        sqlite3_exec(db, transaction.toStdString().c_str(), nullptr, nullptr, nullptr);
    }

    sqlite3_finalize(pInsertStmt);
    sqlite3_close(db);

    std::cout << "elapsed : " << timer.elapsed() << " ms" << std::endl;
    //connect DB
    db = nullptr;
    result = sqlite3_open(dbname.toStdString().c_str(), &db);
    if (SQLITE_OK != result)
    {
        sqlite3_close(db);
        qDebug() << __FUNCTION__ << " - open DB: " << dbname << " failed: " << sqlite3_errstr(result) << endl;
        emit finished();
    }

    result = sqlite3_exec(db, transaction.toStdString().c_str(), nullptr, nullptr, nullptr);
    if (SQLITE_OK != result)
    {
        sqlite3_close(db);
        qDebug() << __FUNCTION__ << " - begin transaction failed: " << sqlite3_errstr(result) << endl;
        emit finished();
    }

    auto pStrSelectSql = "SELECT time, value FROM metadata2";
    sqlite3_stmt *pSelectStmt(nullptr);
    result = sqlite3_prepare_v2(db, pStrSelectSql, strlen(pStrSelectSql), &pSelectStmt, nullptr);
    if (SQLITE_OK != result)
    {
        sqlite3_close(db);
        qDebug() << __FUNCTION__ << " - prepare stmt failed: " << sqlite3_errstr(result) << endl;
        emit finished();
    }
    int i(0);
    while (SQLITE_ROW == sqlite3_step(pSelectStmt))
    {
        ++i;
        qint64 time = sqlite3_column_int64(pSelectStmt, 0);
        std::cout << "time: " << time << std::endl;
        const void* value = sqlite3_column_blob(pSelectStmt, 1);
        if (nullptr == value)
        {
            std::cout << "only time: " << time << std::endl;
            continue;
        }
        size_t size = sqlite3_column_bytes(pSelectStmt, 1);
        std::cout << "size: " << size << std::endl;
        char *b = (char *)(value);
        QByteArray buffer = QByteArray(b, size);
        QDataStream ds(buffer);
        ds.setByteOrder(QDataStream::LittleEndian);
        int iValue; // Since the size you're trying to read appears to be 2 bytes
        ds >> iValue;
        std::cout << "time: " << time << " value: " << iValue << std::endl;
    }
    std::cout << "get number of items: " << i << std::endl;
    if (!success)
    {
        QString transaction("ROLLBACK");
        sqlite3_exec(db, transaction.toStdString().c_str(), nullptr, nullptr, nullptr);
    }
    else
    {
        QString transaction("COMMIT");
        sqlite3_exec(db, transaction.toStdString().c_str(), nullptr, nullptr, nullptr);
    }

    sqlite3_finalize(pInsertStmt);
    sqlite3_close(db);

    emit finished();
}

int main(int argc, char *argv[])
{
    QCoreApplication a(argc, argv);
    Task *task = new Task(&a);
    std::cout << "hello world" << std::endl;
    // This will cause the application to exit when
    // the task signals finished.
    QObject::connect(task, SIGNAL(finished()), &a, SLOT(quit()));

    // This will run the task from the application event loop.
    QTimer::singleShot(0, task, SLOT(run()));
    return a.exec();
}
