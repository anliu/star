// CA4.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"

const int cbCodeId = 0x3EB;
const int btnDecompId = 0x3EC;
const int btnCancelId = 2;

const SIZE_T fbRecordSize = 0xB4;

const LPVOID fileObjectAddr = (LPVOID)0x4101d8;
const SIZE_T fileObjectSize = 0x3A4;

const char *_7zExe = "\"c:\\Program Files\\7-Zip\\7z.exe\" a -sdel output.zip @listfile";

struct FileObject
{
    LPVOID vtable;
    DWORD d2;
    char  error[0x100];
    char  scode[0x20]; //0x108
    DWORD type; //0x128
    DWORD d3;
    DWORD d4;
    DWORD maskmode;
    DWORD markettype; // 0x138
    char  filename[0x100];
    HANDLE dataheap; // 0x23c
    LPVOID filedata;
    DWORD totalsize;
    DWORD datasize;
    HANDLE codeheap;
    LPVOID codetable; //0x250
    DWORD tablesize;
    DWORD codesize;
    DWORD d10;
    HANDLE decodeheap; //0x260
    LPVOID decodedata;
    DWORD decodesize;
    DWORD decodebufsize;
    BYTE  b270_256[0xb4]; // 0x270
    BYTE  b324_40[0x28];  // 0x324
    BYTE  b34c_80[0x50];  // 0x34c
    DWORD d12;
    DWORD d13; // 0x3a0
};

int _tmain_message(int argc, _TCHAR* argv[])
{
    if (argc != 3)
    {
        printf("specify a file name.\n");
        return 0;
    }

    if (!argv[2][0] || argv[2][1])
    {
        printf("specify a market marker (h/s).\n");
        return 0;
    }

    char p[0x100];
    ZeroMemory(p, sizeof(p));
    strcpy_s(p, sizeof(p), argv[1]);

    FILE *paramfp = NULL;
    int err = fopen_s(&paramfp, "dzhtest.exe.param", "rb+");
    if (!err)
    {
        fseek(paramfp, 0, FILE_BEGIN);
        fwrite(p, sizeof(char), sizeof(p), paramfp);
        fseek(paramfp, 0x110, FILE_BEGIN);
        if (argv[2][0] == 'h')
        {
            // sh 0x1e
            fwrite("\x1e", sizeof(char), 1, paramfp);
        }
        else if (argv[2][0] == 's')
        {
            // sz 0x25
            fwrite("\x25", sizeof(char), 1, paramfp);
        }
        fclose(paramfp);
    }
    else
    {
        printf("failed to open param file.\n");
        return -1;
    }

    char fn[0x100];
    GetCurrentDirectoryA(sizeof(fn), fn);
    strcat_s(fn, sizeof(fn), "\\dzhtest.exe");

    STARTUPINFO si;
    PROCESS_INFORMATION pi;
    ZeroMemory(&si, sizeof(si));
    ZeroMemory(&pi, sizeof(pi));
    if (!CreateProcessA(NULL, fn,
            NULL, NULL, FALSE, 0, NULL,
            NULL, // current directory
            &si,
            &pi))
    {
        printf("CreateProcess failed (%d)\n", GetLastError());
        return -2;
    }

    // wait for dzhtest bootstrap
    WaitForSingleObject(pi.hProcess, 3000);

    HWND target = FindWindowA(NULL, "dzhtest");
    if (!target)
    {
        // if it's still not running, give up
        printf("target window not found!\n");
        return -3;
    }
    HWND btnWnd = GetDlgItem(target, btnDecompId);
    HWND cbWnd = GetDlgItem(target, cbCodeId);

    LRESULT r = SendMessage(cbWnd, CB_GETCOUNT, 0, 0);
    char scodesel[32];
    char *fl = new char[r * (sizeof(scodesel) + 1)];
    char *flpointer = fl;

    LPVOID fp;
    FileObject fo;
    SIZE_T bytesRead;
    ReadProcessMemory(pi.hProcess, (LPCVOID)fileObjectAddr, (LPVOID)&fp, sizeof(fp), &bytesRead);
    ReadProcessMemory(pi.hProcess, fp, &fo, fileObjectSize, &bytesRead);

    LPVOID initial = fo.decodedata;

    for (int i = 0; i < r; i++)
    {
        LRESULT re = SendMessage(cbWnd, CB_SETCURSEL, (WPARAM)i, 0);

        re = SendMessage(cbWnd, WM_GETTEXT, (WPARAM)sizeof(scodesel), (LPARAM)scodesel);
        strncpy_s(flpointer, (size_t) re + 1, scodesel, (size_t) re);
        flpointer[re] = '\r';
        flpointer = flpointer + re + 1;

        re = SendMessage(target, WM_COMMAND, MAKEWPARAM(btnDecompId, BN_CLICKED), (LPARAM)btnWnd);

        ReadProcessMemory(pi.hProcess, fp, &fo, fileObjectSize, &bytesRead);

        if (initial && fo.decodedata != initial)
        {
            printf("decoded data buffer changed! %p %p", initial, fo.decodedata);
        }

        BYTE *pData = new BYTE[fo.decodesize];
        ReadProcessMemory(pi.hProcess, fo.decodedata, pData, fo.decodesize, &bytesRead);
        FILE *outfp = NULL;
        err = fopen_s(&outfp, scodesel, "wb");
        if (!err)
        {
            fwrite(pData, 1, bytesRead, outfp);
            fclose(outfp);
        }
        else
        {
            printf("failed to open [%s].\n", scodesel);
        }
        delete pData;
    }

    FILE *outflist = NULL;
    err = fopen_s(&outflist, "listfile", "wb");
    if (!err)
    {
        fwrite(fl, sizeof(char), flpointer - fl, outflist);
        fclose(outflist);
    }
    else
    {
        printf("failed to open [%s].\n", "listfile");
    }
    delete fl;

    SendMessage(target, WM_COMMAND, MAKEWPARAM(btnCancelId, BN_CLICKED), (LPARAM)GetDlgItem(target, btnCancelId));
    WaitForSingleObject( pi.hProcess, INFINITE );
    CloseHandle( pi.hProcess );
    CloseHandle( pi.hThread );

    ZeroMemory(&si, sizeof(si));
    ZeroMemory(&pi, sizeof(pi));
    if (!CreateProcessA(NULL, (LPSTR)_7zExe,
        NULL, NULL, FALSE, 0, NULL,
        NULL, // current directory
        &si,
        &pi))
    {
        printf("CreateProcess failed (%d)\n", GetLastError());
        return -2;
    }

    WaitForSingleObject(pi.hProcess, INFINITE);
    CloseHandle(pi.hProcess);
    CloseHandle(pi.hThread);

    return 0;
}

int _tmain_inject(int argc, _TCHAR* argv[])
{
    if (argc != 3)
    {
        return -1;
    }

    HANDLE processHandle;
    HANDLE threadHandle;
    HMODULE dllHandle;
    DWORD processID;
    FARPROC loadLibraryAddress;
    LPVOID baseAddress;

    processID = (DWORD)_ttol(argv[1]);

    processHandle = OpenProcess(PROCESS_ALL_ACCESS,FALSE,processID);

    if(processHandle == NULL)
    {
        printf("Error unable to open process. Error code: %d", GetLastError());

        return -2;
    }

    printf("Process handle %d is ready",processID);

    dllHandle = GetModuleHandle("Kernel32");

    if(dllHandle == NULL)
    {
        printf("Error unable to allocate kernel32 handle..Error code: %d. Press any key to exit...",GetLastError());
    }

    printf("kernel32 handle is ready\n");

    loadLibraryAddress = GetProcAddress(dllHandle,"LoadLibraryA");

    if(loadLibraryAddress == NULL)
    {
        printf("Cannot get LoadLibraryA() address. Error code: %d. Press any key to exit",GetLastError());

        return -2;
    }

    printf("LoadLibrary() address is ready\n");

    baseAddress = VirtualAllocEx(
        processHandle,
        NULL,
        2048,
        MEM_COMMIT|MEM_RESERVE,
        PAGE_READWRITE);

    if(baseAddress == NULL)
    {
        printf("Error unable to alocate memmory in remote process. Error code: %d. Press any key to exit", GetLastError());

        return 0;
    }

    printf("Memory allocation succeeded\n");

    BOOL isSucceeded = WriteProcessMemory(
        processHandle,
        baseAddress,
        argv[2],
        strlen(argv[2])+1,
        NULL);

    if(isSucceeded == 0)
    {
        printf("Error unable to write memory . Error code: %d Press any key to exit...",GetLastError());

        return 0;
    }

    printf("Argument has been written\n");

    threadHandle = CreateRemoteThread(
        processHandle,
        NULL,
        0,
        (LPTHREAD_START_ROUTINE)loadLibraryAddress,
        baseAddress,
        NULL,
        0);

    if(threadHandle != NULL)
    {
        printf("Remote thread has been created\n");
    }

    return 0;
}

int _tmain(int argc, _TCHAR* argv[])
{
    // return _tmain_inject(argc, argv);

    //FILE *fp = fopen("C:\\BaiduYunDownload\\DN.txt", "rb");
    //fseek(fp, -100L * 1024L * 1024L, FILE_END);
    //if (fp){
    //    for (int i = 0; i < 10; i++)
    //    {
    //        BYTE buf[256];
    //        fread(buf, 1, 256, fp);
    //        printf("%d", buf[0]);
    //    }
    //    fclose(fp);
    //}

    return _tmain_message(argc, argv);
}