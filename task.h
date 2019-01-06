#ifndef TASK_H
#define TASK_H
#include <QtCore>
class Task : public QObject
{
    Q_OBJECT
public:
    Task(QObject *parent = nullptr);
public slots:
    void run();
signals:
    void finished();
};
#endif // TASK_H
